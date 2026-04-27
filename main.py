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
HR_WEEKDAY         = 11  # 1=Mon, 7=Sun
HR_WEATHER_CODE    = 12
HR_WIND_SPEED      = 13
HR_HUMIDITY         = 14
HR_ROOM5_TEMP      = 15
HR_ROOM6_TEMP      = 16
HR_FC_DAY1_CODE    = 17
HR_FC_DAY1_HIGH    = 18
HR_FC_DAY1_LOW     = 19
HR_FC_DAY1_WIND    = 20
HR_FC_DAY1_WEEKDAY = 21
HR_FC_DAY2_CODE    = 22
HR_FC_DAY2_HIGH    = 23
HR_FC_DAY2_LOW     = 24
HR_FC_DAY2_WIND    = 25
HR_FC_DAY2_WEEKDAY = 26
HR_FC_DAY3_CODE    = 27
HR_FC_DAY3_HIGH    = 28
HR_FC_DAY3_LOW     = 29
HR_FC_DAY3_WIND    = 30
HR_FC_DAY3_WEEKDAY = 31

# Coil indices (1-based)
COIL_ALL_LIGHTS_OFF = 1
COIL_COMING_HOME    = 2

WEATHER_ENTITY = "weather.forecast_home"

WEATHER_CODES = {
    "sunny": 1,
    "clear-night": 2,
    "partlycloudy": 3,
    "cloudy": 4,
    "fog": 5,
    "rainy": 6,
    "pouring": 7,
    "snowy": 8,
    "snowy-rainy": 9,
    "hail": 10,
    "lightning": 11,
    "lightning-rainy": 12,
    "windy": 13,
    "windy-variant": 14,
    "exceptional": 15,
}

# HA entities that map to holding registers (temperature sensors)
SENSOR_ENTITIES = {
    "sensor.bathroom_thermostat_air_temperature": HR_BATHROOM_TEMP,
    "sensor.bedroom_thermostat_air_temperature":  HR_BEDROOM_TEMP,
    "sensor.living_room_thermostat_air_temperature": HR_LIVINGROOM_TEMP,
    "sensor.glasshouse_temperature_temperature":  HR_GLASSHOUSE_TEMP,
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


def set_hr_int(index: int, value: int):
    _hr_block.setValues(index, [value])
    log.info(f"[HR {index}] = {value}")


def update_weather_state(state: dict):
    temp = extract_temperature(WEATHER_ENTITY, state)
    set_hr(HR_OUTSIDE_TEMP, temp)

    code = WEATHER_CODES.get(state.get("state", ""), 0)
    if code:
        set_hr_int(HR_WEATHER_CODE, code)

    attrs = state.get("attributes", {})
    try:
        wind_kmh = float(attrs["wind_speed"])
        set_hr(HR_WIND_SPEED, wind_kmh / 3.6)  # km/h → m/s
    except (KeyError, ValueError, TypeError):
        pass
    try:
        set_hr_int(HR_HUMIDITY, int(float(attrs["humidity"])))
    except (KeyError, ValueError, TypeError):
        pass


def update_forecast(forecast: list):
    day_regs = [
        (HR_FC_DAY1_CODE, HR_FC_DAY1_HIGH, HR_FC_DAY1_LOW, HR_FC_DAY1_WIND, HR_FC_DAY1_WEEKDAY),
        (HR_FC_DAY2_CODE, HR_FC_DAY2_HIGH, HR_FC_DAY2_LOW, HR_FC_DAY2_WIND, HR_FC_DAY2_WEEKDAY),
        (HR_FC_DAY3_CODE, HR_FC_DAY3_HIGH, HR_FC_DAY3_LOW, HR_FC_DAY3_WIND, HR_FC_DAY3_WEEKDAY),
    ]
    for i, (code_r, high_r, low_r, wind_r, wd_r) in enumerate(day_regs):
        if i >= len(forecast):
            break
        day = forecast[i]
        code = WEATHER_CODES.get(day.get("condition", ""), 0)
        set_hr_int(code_r, code)
        high = day.get("temperature")
        if high is not None:
            set_hr(high_r, float(high))
        low = day.get("templow")
        if low is not None:
            set_hr(low_r, float(low))
        wind = day.get("wind_speed")
        if wind is not None:
            set_hr(wind_r, float(wind) / 3.6)  # km/h → m/s
        dt_str = day.get("datetime", "")
        if dt_str:
            dt = datetime.fromisoformat(dt_str)
            set_hr_int(wd_r, dt.isoweekday())
    log.info(f"[FORECAST] updated {min(3, len(forecast))} days")


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

                await ws.send_json({"type": "auth", "access_token": HA_TOKEN})

                await ws.send_json({"id": msg_id, "type": "subscribe_events", "event_type": "state_changed"})
                msg_id += 1

                await ws.send_json({"id": msg_id, "type": "get_states"})
                get_states_id = msg_id
                msg_id += 1

                await ws.send_json({
                    "id": msg_id,
                    "type": "weather/subscribe_forecast",
                    "forecast_type": "daily",
                    "entity_id": WEATHER_ENTITY,
                })
                forecast_id = msg_id
                msg_id += 1

                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    data = json.loads(msg.data)

                    if data.get("type") == "result" and data.get("id") == get_states_id:
                        for state in data.get("result", []):
                            eid = state["entity_id"]
                            if eid == WEATHER_ENTITY:
                                update_weather_state(state)
                            elif eid in SENSOR_ENTITIES:
                                temp = extract_temperature(eid, state)
                                set_hr(SENSOR_ENTITIES[eid], temp)

                    elif data.get("id") == forecast_id and data.get("type") == "event":
                        forecast = data.get("event", {}).get("forecast", [])
                        if forecast:
                            update_forecast(forecast)

                    elif data.get("type") == "event":
                        event = data.get("event", {})
                        ed = event.get("data", {})
                        eid = ed.get("entity_id", "")
                        new_state = ed.get("new_state") or {}
                        if eid == WEATHER_ENTITY:
                            update_weather_state(new_state)
                        elif eid in SENSOR_ENTITIES:
                            temp = extract_temperature(eid, new_state)
                            set_hr(SENSOR_ENTITIES[eid], temp)

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
