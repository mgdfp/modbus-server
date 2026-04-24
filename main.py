#!/usr/bin/env python3
import asyncio
import json
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from pymodbus import __version__ as pymodbus_version
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.server import StartAsyncTcpServer

load_dotenv()

HA_URL = os.environ["HA_URL"].rstrip("/")
HA_TOKEN = os.environ["HA_TOKEN"]
HA_HEADERS = {"Authorization": f"Bearer {HA_TOKEN}", "Content-Type": "application/json"}

# ---------------------------------------------------------------------------
# Holding register indices (1-based, as pymodbus uses 1-based addressing)
# ---------------------------------------------------------------------------
HR_OUTSIDE_TEMP   = 1
HR_BATHROOM_TEMP  = 2
HR_BEDROOM_TEMP   = 3
HR_LIVINGROOM_TEMP = 4
HR_GLASSHOUSE_TEMP = 5
HR_HOUR           = 6
HR_MINUTE         = 7
HR_DAY            = 8
HR_MONTH          = 9
HR_YEAR           = 10
HR_WEEKDAY        = 11   # 1=Mon, 7=Sun

# Coil indices (1-based)
COIL_ALL_LIGHTS_OFF = 1
COIL_COMING_HOME    = 2

# HA entities that map to holding registers
SENSOR_ENTITIES = {
    "sensor.bathroom_thermostat_air_temperature": HR_BATHROOM_TEMP,
    "sensor.bedroom_thermostat_air_temperature":  HR_BEDROOM_TEMP,
    "sensor.living_room_thermostat_air_temperature": HR_LIVINGROOM_TEMP,
    "sensor.glasshouse_temperature_temperature":  HR_GLASSHOUSE_TEMP,
    "weather.forecast_home":                      HR_OUTSIDE_TEMP,
}

# HA scripts that map to coils
SCRIPT_COILS = {
    COIL_ALL_LIGHTS_OFF: "script.all_lights_off",
    COIL_COMING_HOME:    "script.coming_home",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Datastore
# ---------------------------------------------------------------------------
_coil_block = ModbusSequentialDataBlock(0, [0] * 200)
_hr_block    = ModbusSequentialDataBlock(0, [0] * 200)

store   = ModbusSlaveContext(
    di=ModbusSequentialDataBlock(0, [0] * 200),
    co=_coil_block,
    hr=_hr_block,
    ir=ModbusSequentialDataBlock(0, [0] * 200),
)
context = ModbusServerContext(slaves=store, single=True)

# Queue: (coil_index, script_entity_id) to fire after a write
_trigger_queue: asyncio.Queue = asyncio.Queue()


def set_hr(index: int, value: float | None):
    if value is None:
        return
    raw = int(round(value * 10))
    _hr_block.setValues(index, [raw])
    log.info(f"[HR {index}] = {raw} ({value})")


def get_coil(index: int) -> bool:
    return bool(_coil_block.getValues(index, 1)[0])


def reset_coil(index: int):
    _coil_block.setValues(index, [0])


# ---------------------------------------------------------------------------
# Coil polling — detect writes from the touchscreen
# ---------------------------------------------------------------------------
async def watch_coils():
    while True:
        for coil_index, script in SCRIPT_COILS.items():
            if get_coil(coil_index):
                log.info(f"[COIL {coil_index}] triggered → {script}")
                reset_coil(coil_index)
                await _trigger_queue.put(script)
        await asyncio.sleep(0.2)


# ---------------------------------------------------------------------------
# HA script caller
# ---------------------------------------------------------------------------
async def script_caller(session: aiohttp.ClientSession):
    while True:
        script = await _trigger_queue.get()
        domain, service = script.split(".", 1)
        url = f"{HA_URL}/api/services/{domain}/turn_on"
        try:
            async with session.post(url, json={"entity_id": script}) as resp:
                log.info(f"[HA] called {script} → {resp.status}")
        except Exception as e:
            log.error(f"[HA] failed to call {script}: {e}")


# ---------------------------------------------------------------------------
# HA WebSocket — subscribe to state changes for sensor entities
# ---------------------------------------------------------------------------
def extract_temperature(entity_id: str, state: dict) -> float | None:
    try:
        if entity_id.startswith("weather."):
            return float(state["attributes"]["temperature"])
        return float(state["state"])
    except (KeyError, ValueError, TypeError):
        return None


async def ha_websocket(session: aiohttp.ClientSession):
    ws_url = HA_URL.replace("http", "ws") + "/api/websocket"
    msg_id = 1

    while True:
        try:
            async with session.ws_connect(ws_url) as ws:
                log.info("[WS] connected to Home Assistant")

                # Authenticate
                await ws.send_json({"type": "auth", "access_token": HA_TOKEN})

                # Subscribe to state_changed events
                await ws.send_json({"id": msg_id, "type": "subscribe_events", "event_type": "state_changed"})
                msg_id += 1

                # Fetch initial states
                await ws.send_json({"id": msg_id, "type": "get_states"})
                get_states_id = msg_id
                msg_id += 1

                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    data = json.loads(msg.data)

                    if data.get("type") == "result" and data.get("id") == get_states_id:
                        for state in data.get("result", []):
                            entity_id = state["entity_id"]
                            if entity_id in SENSOR_ENTITIES:
                                temp = extract_temperature(entity_id, state)
                                set_hr(SENSOR_ENTITIES[entity_id], temp)

                    elif data.get("type") == "event":
                        event = data.get("event", {})
                        ed = event.get("data", {})
                        entity_id = ed.get("entity_id", "")
                        if entity_id in SENSOR_ENTITIES:
                            new_state = ed.get("new_state") or {}
                            temp = extract_temperature(entity_id, new_state)
                            set_hr(SENSOR_ENTITIES[entity_id], temp)

        except Exception as e:
            log.error(f"[WS] error: {e} — reconnecting in 10s")
            await asyncio.sleep(10)


# ---------------------------------------------------------------------------
# Clock — update time/date registers every second
# ---------------------------------------------------------------------------
async def clock_updater():
    while True:
        now = datetime.now()
        _hr_block.setValues(HR_HOUR,    [now.hour])
        _hr_block.setValues(HR_MINUTE,  [now.minute])
        _hr_block.setValues(HR_DAY,     [now.day])
        _hr_block.setValues(HR_MONTH,   [now.month])
        _hr_block.setValues(HR_YEAR,    [now.year])
        _hr_block.setValues(HR_WEEKDAY, [now.isoweekday()])  # 1=Mon, 7=Sun
        await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def run():
    log.info(f"Starting Modbus TCP server on 0.0.0.0:5020 (pymodbus {pymodbus_version})")

    identity = ModbusDeviceIdentification()
    identity.VendorName = "home-assistant-bridge"
    identity.ProductName = "modbus-server"
    identity.MajorMinorRevision = pymodbus_version

    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(headers=HA_HEADERS, connector=connector) as session:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(StartAsyncTcpServer(
                context, identity=identity, address=("0.0.0.0", 5020)
            ))
            tg.create_task(ha_websocket(session))
            tg.create_task(script_caller(session))
            tg.create_task(watch_coils())
            tg.create_task(clock_updater())


if __name__ == "__main__":
    asyncio.run(run())
