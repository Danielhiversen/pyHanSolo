from setuptools import setup

setup(
    name='pyHanSolo',
    packages=['han_solo'],
    install_requires=['crcmod', 'aiohttp'],
    version='0.1.0',
    description='A python3 library to communicate with HanSolo',
    python_requires='>=3.5.3',
    author='Daniel Hoyer Iversen',
    author_email='mail@dahoiv.net',
    url='https://github.com/Danielhiversen/pyHanSolo',
    license="MIT",
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Home Automation',
        'Topic :: Software Development :: Libraries :: Python Modules'
        ]
)
