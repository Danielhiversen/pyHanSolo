"""Subscription manager for Tibber Han Solo."""
import asyncio
import base64
import crcmod
import logging
import struct
from datetime import datetime
from time import time

import aiohttp

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

STATE_STARTING = "starting"
STATE_RUNNING = "running"
STATE_STOPPED = "stopped"

HOST = '192.168.1.9'
PORT = 9876

FEND = 126  # 7e


class SubscriptionManager:
    """Subscription manager."""

    # pylint: disable=too-many-instance-attributes

    def __init__(self, loop, session, url):
        """Create resources for websocket communication."""
        self.loop = loop
        self._url = url
        self._session = session
        self.subscriptions = []
        self._state = None
        self.websocket = None
        self._retry_timer = None
        self._client_task = None
        self._wait_time_before_retry = 15
        self._show_connection_error = True
        self._is_running = False
        self._crc = crcmod.mkCrcFun(0x11021, rev=True, initCrc=0xffff, xorOut=0x0000)
        self._decoders = [decode_kaifa, ]
        self._default_decoder = self._decoders[0]

    def start(self):
        """Start websocket."""
        _LOGGER.debug("Start state %s.", self._state)
        if self._state == STATE_RUNNING:
            return
        self._state = STATE_STARTING
        self._cancel_client_task()
        self._client_task = self.loop.create_task(self.running())

    @property
    def is_running(self):
        """Return if client is running or not."""
        return self._is_running

    async def running(self):
        """Start websocket connection."""
        # pylint: disable=too-many-branches, too-many-statements
        await self._close_websocket()
        try:
            _LOGGER.debug("Starting")
            self.websocket = await self._session.ws_connect(self._url)

            self._state = STATE_RUNNING
            _LOGGER.debug("Running")
            k = 0
            while self._state == STATE_RUNNING:
                try:
                    msg = await asyncio.wait_for(self.websocket.receive(), timeout=30)
                except asyncio.TimeoutError:
                    k += 1
                    if k > 10:
                        if self._show_connection_error:
                            _LOGGER.error("No data, reconnecting.")
                            self._show_connection_error = False
                        self._is_running = False
                        return
                    _LOGGER.debug(
                        "No websocket data in 30 seconds, checking the connection."
                    )
                    try:
                        pong_waiter = self.websocket.ping()
                        await asyncio.wait_for(pong_waiter, timeout=10)
                    except asyncio.TimeoutError:
                        if self._show_connection_error:
                            _LOGGER.error(
                                "No response to ping in 10 seconds, reconnecting."
                            )
                            self._show_connection_error = False
                        self._is_running = False
                        return
                    continue
                if msg.type in [aiohttp.WSMsgType.closed, aiohttp.WSMsgType.error]:
                    print(msg, self._state)
                    return
                k = 0
                self._is_running = True
                await self._process_msg(msg)
                self._show_connection_error = True
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error("Unexpected error", exc_info=True)
        finally:
            await self._close_websocket()
            if self._state != STATE_STOPPED:
                _LOGGER.debug("Reconnecting")
                self._state = STATE_STOPPED
                self.retry()
            _LOGGER.debug("Closing running task.")

    async def stop(self, timeout=10):
        """Close websocket connection."""
        _LOGGER.debug("Stopping client.")
        start_time = time()
        self._cancel_retry_timer()
        self._state = STATE_STOPPED
        while (
                timeout > 0
                and self.websocket is not None
                and not self.websocket.closed
                and (time() - start_time) < timeout
        ):
            await asyncio.sleep(0.1, loop=self.loop)

        await self._close_websocket()
        self._cancel_client_task()
        _LOGGER.debug("Server connection is stopped")

    def retry(self):
        """Retry to connect to websocket."""
        _LOGGER.debug("Retry, state: %s", self._state)
        if self._state in [STATE_STARTING, STATE_RUNNING]:
            _LOGGER.debug("Skip retry since state: %s", self._state)
            return
        _LOGGER.debug("Cancel retry timer")
        self._cancel_retry_timer()
        self._state = STATE_STARTING
        _LOGGER.debug("Restart")
        self._retry_timer = self.loop.call_later(
            self._wait_time_before_retry, self.start
        )
        _LOGGER.debug(
            "Reconnecting to server in %i seconds.", self._wait_time_before_retry
        )

    async def subscribe(self, callback):
        """Add a new subscription."""
        if callback in self.subscriptions:
            return
        self.subscriptions.append(callback)

    async def unsubscribe(self, callback):
        """Unsubscribe."""
        if callback not in self.subscriptions:
            return
        self.subscriptions.remove(callback)

    async def _close_websocket(self):
        if self.websocket is None:
            return
        try:
            await self.websocket.close()
        finally:
            self.websocket = None

    async def _process_msg(self, msg):
        """Process received msg."""
        _LOGGER.debug("Recv, %s", msg)
        data = msg.data
        if data is None:
            return

        if msg.type == aiohttp.WSMsgType.BINARY:
            if (len(data) < 9
                    or not data[0] == FEND
                    or not data[-1] == FEND):
                _LOGGER.error("Invalid data %s", data)
                return
            data = data[1:-1]
            crc = self._crc(data[:-2])
            crc ^= 0xffff
            if crc != struct.unpack("<H", data[-2:])[0]:
                _LOGGER.error("Invalid crc %s %s", crc, struct.unpack("<H", data[-2:])[0])

            buf = ''.join('{:02x}'.format(x).upper() for x in data)
            decoded_data = self._default_decoder(buf)
            if decoded_data is None:
                for decoder in self._decoders:
                    decoded_data = decoder(buf)
                    if decoded_data is not None:
                        self._default_decoder = decoder
                        break
            print(decoded_data)

        for callback in self.subscriptions:
            await callback(decoded_data)

    def _cancel_retry_timer(self):
        if self._retry_timer is None:
            return
        try:
            self._retry_timer.cancel()
        finally:
            self._retry_timer = None

    def _cancel_client_task(self):
        if self._client_task is None:
            return
        try:
            self._client_task.cancel()
        finally:
            self._client_task = None


def decode_kaifa(buf):
    if buf[10:12] != '10':
        _LOGGER.error("Unknown control field %s", buf[10:12])
        return None
    if int(buf[2:4], 16)*2 != len(buf):
        _LOGGER.error("Invalid length %s, %s", int(buf[2:4], 16)*2, len(buf))
        return None
    buf = buf[32:]
    try:
        txt_buf = buf[28:]
        if txt_buf[:2] != '02':
            _LOGGER.error("Unknown data %s", buf[0])
            return None

        year = int(buf[4:8], 16)
        month = int(buf[8:10], 16)
        day = int(buf[10:12], 16)
        hour = int(buf[14:16], 16)
        minute = int(buf[16:18], 16)
        second = int(buf[18:20], 16)
        date = "%02d%02d%02d_%02d%02d%02d" % (second, minute, hour, day, month, year)

        res = {}
        res['time_stamp'] = datetime.strptime(date, '%S%M%H_%d%m%Y')

        pkt_type = txt_buf[2:4]
        txt_buf = txt_buf[4:]
        if pkt_type == '01':
            res['Effect'] = int(txt_buf[2:10], 16)
        elif pkt_type in ['09', '0E']:
            res['Version identifier'] = base64.b16decode(txt_buf[4:18]).decode("utf-8")
            txt_buf = txt_buf[18:]
            res['Meter-ID'] = base64.b16decode(txt_buf[4:36]).decode("utf-8")
            txt_buf = txt_buf[36:]
            res['Meter type'] = base64.b16decode(txt_buf[4:20]).decode("utf-8")
            txt_buf = txt_buf[20:]
            res['Effect'] = int(txt_buf[2:10], 16)
            if pkt_type == '0E':
                txt_buf = txt_buf[10:]
                txt_buf = txt_buf[78:]
                res['Cumulative_hourly_active_import_energy'] = int(txt_buf[2:10], 16)
                txt_buf = txt_buf[10:]
                res['Cumulative_hourly_active_export_energy'] = int(txt_buf[2:10], 16)
                txt_buf = txt_buf[10:]
                res['Cumulative_hourly_reactive_import_energy'] = int(txt_buf[2:10], 16)
                txt_buf = txt_buf[10:]
                res['Cumulative_hourly_reactive_export_energy'] = int(txt_buf[2:10], 16)
        else:
            _LOGGER.warning("Unknown type %s", pkt_type)
            return None
    except ValueError:
        _LOGGER.error("Failed", exc_info=True)
        return None
    return res


if __name__ == '__main__':
    async def _main():
        session = aiohttp.ClientSession()
        url = "ws://{}:{}".format(HOST, PORT)
        sub_manager = SubscriptionManager(loop, session, url)
        sub_manager.start()
        while True:
            await asyncio.sleep(3600)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_main())

