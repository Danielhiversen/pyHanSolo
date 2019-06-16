"""Subscription manager for Tibber Han Solo."""
import asyncio
import logging
from time import time

import aiohttp

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

STATE_STARTING = "starting"
STATE_RUNNING = "running"
STATE_STOPPED = "stopped"

HOST = '192.168.80.138'
PORT = 9876


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
        print(time())
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
                    # print(msg, msg.type )
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
        # _LOGGER.debug("Recv, %s", msg)
        data = msg.data
        if data is None:
            return

        for callback in self.subscriptions:
            await callback(data)

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


if __name__ == '__main__':
    async def _main():
        session = aiohttp.ClientSession()
        url = "ws://{}:{}".format(HOST, PORT)
        sub_manager = SubscriptionManager(loop, session, url)
        sub_manager.start()
        while True:
            print("a")
            await asyncio.sleep(3600)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_main())


