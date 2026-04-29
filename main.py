#!/usr/bin/env python3
import asyncio
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

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

YR_LAT = os.environ["YR_LAT"]
YR_LON = os.environ["YR_LON"]
YR_URL = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={YR_LAT}&lon={YR_LON}"
YR_HEADERS = {"User-Agent": "home-modbus-bridge/1.0"}
DISPLAY_TZ = ZoneInfo("Europe/Oslo")

# ---------------------------------------------------------------------------
# Holding register indices (1-based)
# ---------------------------------------------------------------------------
HR_OUTSIDE_TEMP    = 1
HR_BATHROOM_TEMP   = 2
HR_BEDROOM_TEMP    = 3
HR_LIVINGROOM_TEMP = 4
HR_GLASSHOUSE_TEMP = 5
HR_HOUR            = 6
HR_MINUTE          = 7
HR_DAY             = 8
HR_MONTH           = 9
HR_YEAR            = 10
HR_WEEKDAY         = 11  # 1=Mon, 7=Sun
HR_WEATHER_CODE    = 12
HR_WIND_SPEED      = 13
HR_HUMIDITY        = 14
HR_ROOM5_TEMP      = 15
HR_ROOM6_TEMP      = 16
# Daily summary (HR 17-31) — kept for backward compat with current page1 display
HR_FC_DAY1_CODE    = 17
HR_FC_DAY1_HIGH    = 18
HR_FC_DAY1_LOW     = 19
HR_FC_DAY1_PRECIP  = 20
HR_FC_DAY1_WEEKDAY = 21
HR_FC_DAY2_CODE    = 22
HR_FC_DAY2_HIGH    = 23
HR_FC_DAY2_LOW     = 24
HR_FC_DAY2_PRECIP  = 25
HR_FC_DAY2_WEEKDAY = 26
HR_FC_DAY3_CODE    = 27
HR_FC_DAY3_HIGH    = 28
HR_FC_DAY3_LOW     = 29
HR_FC_DAY3_PRECIP  = 30
HR_FC_DAY3_WEEKDAY = 31
# 6-hour slot forecast (HR 32-67): 3 days × 4 slots × 3 values
# Slots: S0=00-06, S1=06-12, S2=12-18, S3=18-24  (local time)
HR_FC_D1_S0_CODE = 32;  HR_FC_D1_S0_TEMP = 33;  HR_FC_D1_S0_PRECIP = 34
HR_FC_D1_S1_CODE = 35;  HR_FC_D1_S1_TEMP = 36;  HR_FC_D1_S1_PRECIP = 37
HR_FC_D1_S2_CODE = 38;  HR_FC_D1_S2_TEMP = 39;  HR_FC_D1_S2_PRECIP = 40
HR_FC_D1_S3_CODE = 41;  HR_FC_D1_S3_TEMP = 42;  HR_FC_D1_S3_PRECIP = 43
HR_FC_D2_S0_CODE = 44;  HR_FC_D2_S0_TEMP = 45;  HR_FC_D2_S0_PRECIP = 46
HR_FC_D2_S1_CODE = 47;  HR_FC_D2_S1_TEMP = 48;  HR_FC_D2_S1_PRECIP = 49
HR_FC_D2_S2_CODE = 50;  HR_FC_D2_S2_TEMP = 51;  HR_FC_D2_S2_PRECIP = 52
HR_FC_D2_S3_CODE = 53;  HR_FC_D2_S3_TEMP = 54;  HR_FC_D2_S3_PRECIP = 55
HR_FC_D3_S0_CODE = 56;  HR_FC_D3_S0_TEMP = 57;  HR_FC_D3_S0_PRECIP = 58
HR_FC_D3_S1_CODE = 59;  HR_FC_D3_S1_TEMP = 60;  HR_FC_D3_S1_PRECIP = 61
HR_FC_D3_S2_CODE = 62;  HR_FC_D3_S2_TEMP = 63;  HR_FC_D3_S2_PRECIP = 64
HR_FC_D3_S3_CODE = 65;  HR_FC_D3_S3_TEMP = 66;  HR_FC_D3_S3_PRECIP = 67
# Per-slot opacity for today's column (HR 68-70): 100 = visible, 30 = dimmed
HR_SLOT0_OPACITY = 68
HR_SLOT1_OPACITY = 69
HR_SLOT2_OPACITY = 70

# Coil indices (1-based)
COIL_ALL_LIGHTS_OFF = 1
COIL_COMING_HOME    = 2

# HA entities that map to holding registers (indoor temperature sensors only)
SENSOR_ENTITIES = {
    "sensor.bathroom_thermostat_air_temperature":    HR_BATHROOM_TEMP,
    "sensor.bedroom_thermostat_air_temperature":     HR_BEDROOM_TEMP,
    "sensor.living_room_thermostat_air_temperature": HR_LIVINGROOM_TEMP,
    "sensor.glasshouse_temperature_temperature":     HR_GLASSHOUSE_TEMP,
}

# HA scripts that map to coils
SCRIPT_COILS = {
    COIL_ALL_LIGHTS_OFF: "script.all_lights_off",
    COIL_COMING_HOME:    "script.coming_home",
}

# yr.no symbol code → display code (1-based: 1 = first image in MultiStateImageWgt)
# Suffix _day / _night / _polartwilight is stripped before lookup
# Code 2 is reserved for clearsky at night (weather_clearnight.svg)
_CODE_NAMES = {
    0: "clearday", 1: "clearnight", 2: "pcloudy", 3: "cloudy",
    4: "fog", 5: "rain", 6: "heavyrain", 7: "snow", 8: "sleet",
    10: "thunder", 11: "tstorm",
}
YR_SYMBOL_MAP = {
    "clearsky":                        0,   # weather_clear
    "fair":                            0,   # weather_clear
    "partlycloudy":                    2,   # weather_partlycloudy
    "cloudy":                          3,   # weather_cloudy
    "fog":                             4,   # weather_fog
    "lightrain":                       5,   # weather_rain
    "lightrainshowers":                5,
    "rain":                            5,
    "rainshowers":                     5,
    "heavyrain":                       6,   # weather_heavyrain
    "heavyrainshowers":                6,
    "lightsleet":                      8,   # weather_sleet
    "lightsleetshowers":               8,
    "sleet":                           8,
    "sleetshowers":                    8,
    "heavysleet":                      8,
    "heavysleetshowers":               8,
    "lightsnow":                       7,   # weather_snow
    "lightsnowshowers":                7,
    "snow":                            7,
    "snowshowers":                     7,
    "heavysnow":                       7,
    "heavysnowshowers":                7,
    "lightrainandthunder":             11,  # weather_thunderrain
    "lightrainshowersandthunder":      11,
    "rainandthunder":                  11,
    "rainshowersandthunder":           11,
    "heavyrainandthunder":             11,
    "heavyrainshowersandthunder":      11,
    "lightsleetandthunder":            11,
    "lightsleetshowersandthunder":     11,
    "sleetandthunder":                 11,
    "sleetshowersandthunder":          11,
    "heavysleetandthunder":            11,
    "heavysleetshowersandthunder":     11,
    "lightsnowandthunder":             11,
    "lightsnowshowersandthunder":      11,
    "snowandthunder":                  11,
    "snowshowersandthunder":           11,
    "heavysnowandthunder":             11,
    "heavysnowshowersandthunder":      11,
}

# Slot and daily register tables indexed by day (0-2) and slot (0-3)
_SLOT_REGS = [
    [
        (HR_FC_D1_S0_CODE, HR_FC_D1_S0_TEMP, HR_FC_D1_S0_PRECIP),
        (HR_FC_D1_S1_CODE, HR_FC_D1_S1_TEMP, HR_FC_D1_S1_PRECIP),
        (HR_FC_D1_S2_CODE, HR_FC_D1_S2_TEMP, HR_FC_D1_S2_PRECIP),
        (HR_FC_D1_S3_CODE, HR_FC_D1_S3_TEMP, HR_FC_D1_S3_PRECIP),
    ],
    [
        (HR_FC_D2_S0_CODE, HR_FC_D2_S0_TEMP, HR_FC_D2_S0_PRECIP),
        (HR_FC_D2_S1_CODE, HR_FC_D2_S1_TEMP, HR_FC_D2_S1_PRECIP),
        (HR_FC_D2_S2_CODE, HR_FC_D2_S2_TEMP, HR_FC_D2_S2_PRECIP),
        (HR_FC_D2_S3_CODE, HR_FC_D2_S3_TEMP, HR_FC_D2_S3_PRECIP),
    ],
    [
        (HR_FC_D3_S0_CODE, HR_FC_D3_S0_TEMP, HR_FC_D3_S0_PRECIP),
        (HR_FC_D3_S1_CODE, HR_FC_D3_S1_TEMP, HR_FC_D3_S1_PRECIP),
        (HR_FC_D3_S2_CODE, HR_FC_D3_S2_TEMP, HR_FC_D3_S2_PRECIP),
        (HR_FC_D3_S3_CODE, HR_FC_D3_S3_TEMP, HR_FC_D3_S3_PRECIP),
    ],
]

_DAILY_REGS = [
    (HR_FC_DAY1_CODE, HR_FC_DAY1_HIGH, HR_FC_DAY1_LOW, HR_FC_DAY1_PRECIP, HR_FC_DAY1_WEEKDAY),
    (HR_FC_DAY2_CODE, HR_FC_DAY2_HIGH, HR_FC_DAY2_LOW, HR_FC_DAY2_PRECIP, HR_FC_DAY2_WEEKDAY),
    (HR_FC_DAY3_CODE, HR_FC_DAY3_HIGH, HR_FC_DAY3_LOW, HR_FC_DAY3_PRECIP, HR_FC_DAY3_WEEKDAY),
]

_SLOT_HOURS = [0, 6, 12, 18]

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

_trigger_queue: asyncio.Queue = asyncio.Queue()


def set_hr(index: int, value: float | None):
    if value is None:
        return
    raw = int(round(value * 10))
    _hr_block.setValues(index, [raw])
    log.debug(f"[HR {index}] = {raw} ({value})")


def set_hr_int(index: int, value: int):
    _hr_block.setValues(index, [value])
    log.debug(f"[HR {index}] = {value}")


def get_coil(index: int) -> bool:
    return bool(_coil_block.getValues(index, 1)[0])


def reset_coil(index: int):
    _coil_block.setValues(index, [0])


# ---------------------------------------------------------------------------
# yr.no forecast
# ---------------------------------------------------------------------------
def _yr_code(symbol: str) -> int:
    is_night = symbol.endswith("_night") or symbol.endswith("_polartwilight")
    for suffix in ("_day", "_night", "_polartwilight"):
        if symbol.endswith(suffix):
            symbol = symbol[: -len(suffix)]
            break
    code = YR_SYMBOL_MAP.get(symbol, 0)
    if is_night and code == 0:
        return 1  # clearsky/fair at night → weather_clearnight (moon)
    return code


def _process_yr(data: dict):
    timeseries = data["properties"]["timeseries"]

    ts_map: dict[datetime, dict] = {}
    for entry in timeseries:
        dt = datetime.fromisoformat(entry["time"].replace("Z", "+00:00"))
        ts_map[dt.replace(minute=0, second=0, microsecond=0)] = entry

    local_tz  = DISPLAY_TZ
    now_local = datetime.now(DISPLAY_TZ)
    now_utc   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    # Current conditions
    if now_utc in ts_map:
        entry = ts_map[now_utc]
        d = entry["data"]["instant"]["details"]
        set_hr(HR_OUTSIDE_TEMP, d.get("air_temperature"))
        set_hr(HR_WIND_SPEED,   d.get("wind_speed"))  # yr.no already m/s
        hum = d.get("relative_humidity")
        if hum is not None:
            set_hr_int(HR_HUMIDITY, int(round(hum)))
        sym = (entry["data"].get("next_1_hours") or {}).get("summary", {}).get("symbol_code", "")
        code = _yr_code(sym)
        if code:
            set_hr_int(HR_WEATHER_CODE, code)

    today_local = now_local.date()

    for day_i, (slot_regs, daily_regs) in enumerate(zip(_SLOT_REGS, _DAILY_REGS)):
        day_date  = today_local + timedelta(days=day_i)
        code_r, high_r, low_r, precip_r, wd_r = daily_regs

        set_hr_int(wd_r, date(day_date.year, day_date.month, day_date.day).isoweekday())

        temps: list[float] = []
        precip_total = 0.0
        noon_code  = 0
        first_code = 0

        for slot_i, (sc_r, st_r, sp_r) in enumerate(slot_regs):
            hour     = _SLOT_HOURS[slot_i]
            local_dt = datetime(day_date.year, day_date.month, day_date.day,
                                hour, tzinfo=local_tz)
            utc_dt   = local_dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

            entry = ts_map.get(utc_dt)
            if entry is None:
                utc_slot_end = utc_dt + timedelta(hours=6)
                start = max(utc_dt, now_utc) if utc_dt < now_utc else utc_dt
                candidate = start
                while candidate < utc_slot_end:
                    if candidate in ts_map:
                        entry = ts_map[candidate]
                        break
                    candidate += timedelta(hours=1)
            if entry is None:
                continue

            d   = entry["data"]["instant"]["details"]
            n6  = entry["data"].get("next_6_hours") or {}
            n1  = entry["data"].get("next_1_hours") or {}
            sym = (n6.get("summary") or n1.get("summary") or {}).get("symbol_code", "")
            code = _yr_code(sym)

            set_hr_int(sc_r, code)
            temp = d.get("air_temperature")
            set_hr(st_r, temp)

            precip = (n6.get("details") or n1.get("details") or {}).get("precipitation_amount")
            set_hr(sp_r, precip)

            if temp is not None:
                temps.append(float(temp))
            if precip is not None:
                precip_total += float(precip)
            if code and not first_code:
                first_code = code
            if hour == 12 and code:
                noon_code = code

        day_code = noon_code or first_code
        if day_code:
            set_hr_int(code_r, day_code)
        if temps:
            set_hr(high_r, max(temps))
            set_hr(low_r,  min(temps))
        set_hr(precip_r, precip_total)

    for day_i2, slot_regs2 in enumerate(_SLOT_REGS):
        parts = []
        for slot_i2, (sc_r2, st_r2, sp_r2) in enumerate(slot_regs2):
            code2 = _hr_block.getValues(sc_r2, 1)[0]
            temp2 = _hr_block.getValues(st_r2, 1)[0] / 10.0
            prcp2 = _hr_block.getValues(sp_r2, 1)[0] / 10.0
            name2 = _CODE_NAMES.get(code2, str(code2))
            parts.append(f"S{slot_i2}={name2}/{temp2:.1f}°C/{prcp2:.1f}mm")
        log.info(f"[YR] D{day_i2+1}: {' '.join(parts)}")
    log.info(f"[YR] updated forecast for {today_local}")


async def yr_poller():
    async with aiohttp.ClientSession(headers=YR_HEADERS) as session:
        while True:
            try:
                async with session.get(YR_URL) as resp:
                    if resp.status == 200:
                        _process_yr(await resp.json(content_type=None))
                    else:
                        log.warning(f"[YR] HTTP {resp.status}")
            except Exception as e:
                log.error(f"[YR] {e}")
            await asyncio.sleep(1800)  # met.no asks for max 1 req / 30 min per location


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
# HA WebSocket — indoor temperature sensors only
# ---------------------------------------------------------------------------
def _extract_temp(state: dict) -> float | None:
    try:
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

                await ws.send_json({"id": msg_id, "type": "subscribe_events",
                                    "event_type": "state_changed"})
                msg_id += 1

                await ws.send_json({"id": msg_id, "type": "get_states"})
                get_states_id = msg_id
                msg_id += 1

                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    data = json.loads(msg.data)

                    if data.get("type") == "result" and data.get("id") == get_states_id:
                        for state in data.get("result", []):
                            eid = state["entity_id"]
                            if eid in SENSOR_ENTITIES:
                                set_hr(SENSOR_ENTITIES[eid], _extract_temp(state))

                    elif data.get("type") == "event":
                        ed  = data.get("event", {}).get("data", {})
                        eid = ed.get("entity_id", "")
                        if eid in SENSOR_ENTITIES:
                            new_state = ed.get("new_state") or {}
                            set_hr(SENSOR_ENTITIES[eid], _extract_temp(new_state))

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
        _hr_block.setValues(HR_WEEKDAY, [now.isoweekday()])
        slot = now.hour // 6
        s0 = 30 if slot > 0 else 100
        s1 = 30 if slot > 1 else 100
        s2 = 30 if slot > 2 else 100
        _hr_block.setValues(HR_SLOT0_OPACITY, [s0])
        _hr_block.setValues(HR_SLOT1_OPACITY, [s1])
        _hr_block.setValues(HR_SLOT2_OPACITY, [s2])
        if now.second == 0:
            log.info(f"slot={slot} opacity: s0={s0} s1={s1} s2={s2}")
        await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def run():
    log.info(f"Starting Modbus TCP server on 0.0.0.0:5020 (pymodbus {pymodbus_version})")

    identity = ModbusDeviceIdentification()
    identity.VendorName  = "home-assistant-bridge"
    identity.ProductName = "modbus-server"
    identity.MajorMinorRevision = pymodbus_version

    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(headers=HA_HEADERS, connector=connector) as ha_session:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(StartAsyncTcpServer(
                context, identity=identity, address=("0.0.0.0", 5020)
            ))
            tg.create_task(ha_websocket(ha_session))
            tg.create_task(script_caller(ha_session))
            tg.create_task(watch_coils())
            tg.create_task(clock_updater())
            tg.create_task(yr_poller())


if __name__ == "__main__":
    asyncio.run(run())
