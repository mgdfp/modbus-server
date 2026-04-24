#!/usr/bin/env python3
import asyncio
import logging
import signal
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pymodbus import __version__ as pymodbus_version
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.server import StartAsyncTcpServer

LOG_PATH = Path.home() / ".local" / "share" / "modbus-server" / "modbus-server.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

log = logging.getLogger()
log.setLevel(logging.INFO)

handler = RotatingFileHandler(LOG_PATH, maxBytes=1_000_000, backupCount=5)
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)-20s %(message)s",
    "%Y-%m-%d %H:%M:%S",
))
log.addHandler(handler)
log.addHandler(logging.StreamHandler())


class LoggingDataBlock(ModbusSequentialDataBlock):
    def setValues(self, address, values):
        log.info(f"[WRITE] address={address} values={values}")
        super().setValues(address, values)


store = ModbusSlaveContext(
    di=ModbusSequentialDataBlock(0, [0] * 200),
    co=LoggingDataBlock(0, [0] * 200),
    hr=LoggingDataBlock(0, [123] * 200),
    ir=ModbusSequentialDataBlock(0, [0] * 200),
)
context = ModbusServerContext(slaves=store, single=True)

identity = ModbusDeviceIdentification()
identity.VendorName = "MyCompany"
identity.ProductCode = "PM"
identity.ProductName = "modbus-server"
identity.ModelName = "modbus-server"
identity.MajorMinorRevision = pymodbus_version


async def run():
    log.info(f"Starting Modbus TCP server on 0.0.0.0:5020 (pymodbus {pymodbus_version})")
    server = await StartAsyncTcpServer(
        context,
        identity=identity,
        address=("0.0.0.0", 5020),
    )
    return server


if __name__ == "__main__":
    asyncio.run(run())
