"""
Tara-Rora-Server
-----------------
FastAPI application that receives LoRaWAN uplink messages (via HTTP), decodes
specific frame formats, optionally persists readings to a MySQL database, and
stores raw/decoded messages as JSON files on disk for auditing.

Key features:
- Bridges Python's standard logging (including uvicorn/fastapi) into Loguru.
- Supports HTTP Basic authentication for ingestion endpoint.
- Decoders for:
  * 0x46 Adeunis-like frame (pulse counters on two channels)
  * 0x9d LR9 frame (custom format: meter index, alarms, factor)
  * 0x30 Pulse 3 keep-alive (min/max over last 24h per channel)
- Database helpers to check device existence and insert measurements.

Notes:
- Configuration is loaded once on module import using a helper from LogLibrary.
- Many safeguards are implemented to avoid crashes and to ensure logs are rich
  and actionable in production.
"""

from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.concurrency import run_in_threadpool
from LogLibrary import Load_Config, Loguru_Logging
from datetime import datetime, timedelta, timezone
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from typing import Any, Dict
import mysql.connector
import logging
import uvicorn
import secrets
import base64
import json
import os

# ----------------------- Configuration Values -----------------------
Program_Name = "Tara-Rora-Server"
Program_Version = "1.7"

# Default configuration used when no external config is present. The
# Load_Config helper can merge/override these values from an external source
# (e.g., config.json). Each section is documented inline for clarity.
default_config = {
    "DB_config": {
        # MySQL connection details
        "mysql_host": "",
        "mysql_port": 3306,
        "mysql_user": "",
        "mysql_Pass": "",
        "mysql_database": "",
    },
    "API_Listen": {
        # FastAPI/uvicorn bind host/port
        "host": "0.0.0.0",
        "port": 8000,
        "workers": 1
    },
    "Auth": {
        # HTTP Basic auth credentials for /ingest endpoint
        "username": "aurten",
        "password": "aurten@2025"
    },
    # Logging configuration for Loguru_Logging helper
    "log_Level": "DEBUG",
    "Log_Console": 1,
    "log_Backup": 90,
    "Log_Size": "10 MB"
}

# ---------------------------------------------------------------------
# Load configuration and initialize Loguru logger once at module import time
config = Load_Config(default_config, Program_Name)
logger = Loguru_Logging(config, Program_Name, Program_Version)

# Mapping จากไฟล์ Excel: Status A Common Alarms
# ===== LR9 Alarm mappings =====
# Default mapping (ใช้ตรงตาม Excel เผื่อกรณี config ภายนอกไม่มี key นี้)
ALARM_BITS_DEFAULT_LR9 = {
    0:  "Tamper",
    1:  "PUS Failure (Cut Wire)",
    2:  "Backflow (CCW)",
    3:  "Leak",
    4:  "Poor RF",
    5:  "Low Battery",
    6:  "Field prog",
    7:  "System",
    8:  "Low temperature",
    9:  "Tilt",
    10: "Qmax",
    11: "Burst Pipe",
    12: "Empty Pipe",
    13: "Sensus Low Battery",
    14: "Sensus Invalid Read",
    15: "Reserved",
}

EXT_ALARM_BITS_DEFAULT_LR9 = {
    0: "Reset Watch Dog (WD)",
    1: "HW Reset",
    2: "Reset On Power (POR)",
    3: "HW Failure",
    4: "Box switch Open",
    5: "Charging Failure",
    6: "Power monitor Failure",
    7: "Low Pressure (LP)",
    8: "High Pressure (HP)",
    9:  "Reserved",
    10: "Reserved",
    11: "Reserved",
    12: "Reserved",
    13: "Reserved",
    14: "Reserved",
    15: "Reserved",
}

# ----------------------- Bridge: Python logging -> Loguru -----------------------
class LoguruHandler(logging.Handler):
    """Redirect standard logging records to Loguru.

    This ensures libraries that use the stdlib `logging` (like uvicorn and
    FastAPI) are routed through the unified Loguru sink configured above.
    """
    def emit(self, record: logging.LogRecord) -> None:
        """Map a stdlib logging record to a Loguru log call.

        - Preserves the original logger name via a bound extra field
          (logger_name) so you can filter/format by source.
        - Uses `opt(depth=6, exception=record.exc_info)` to maintain the
          original call site and exception tracebacks.
        """
        try:
            level = record.levelname
        except Exception:
            level = "INFO"

        # Bind the originating logger name (uvicorn, fastapi, etc.) as extra context
        log = logger.bind(logger_name=record.name)

        log.opt(
            depth=6,
            exception=record.exc_info
        ).log(level, record.getMessage())

# Use LoguruHandler as the primary handler for standard logging
logging.basicConfig(handlers=[LoguruHandler()], level=0, force=True)

# Attach uvicorn/fastapi loggers to use LoguruHandler
for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
    _log = logging.getLogger(name)
    _log.handlers = [LoguruHandler()]
    _log.propagate = False

# ----------------------- Helpers / Decoders -----------------------
def hex_to_bin(hex_str):
    """Convert a hex string to a zero-padded binary string.

    Example: "0f" -> "00001111"
    """
    return bin(int(hex_str, 16))[2:].zfill(len(hex_str) * 4)


def hex_to_decimal(hex_str):
    """Convert a hex string to a decimal integer."""
    return int(hex_str, 16)


def decode_alarm_bits(hex_alarm: str, mapping: dict = None):
    """Expand a 4-hex-digit (16-bit) alarm field into details.

    mapping: dict ของ bit -> ชื่อ alarm (ถ้าไม่ส่ง จะใช้ ALARM_BITS)

    Returns:
    - bits: dict ของ per-bit details (index, value, human-readable name, active flag)
    - active: list ของ human-readable alarm names ที่ถูก set (value == 1)
    """
    if mapping is None:
        mapping = ALARM_BITS

    value = int(hex_alarm, 16)
    bits = {}
    active = []

    for bit in range(16):
        bit_val = (value >> bit) & 0x01
        name = mapping.get(bit, f"Bit{bit}")
        bits[bit] = {
            "bit": bit,
            "value": bit_val,
            "name": name,
            "active": bool(bit_val),
        }
        if bit_val and name:
            active.append(name)

    return bits, active


def decode_LR9(hex_payload: str) -> dict:
    """Decode an LR9 custom frame (prefix 0x9d).

    Expected layout (by nibble/byte indexes in the hex string):
    - 0:2   FrameType
    - 2:4   UnitId
    - 4:8   Alarm (16 bits)
    - 8:12  ExtendAlarm (16 bits)
    - 12:20 MeterIndex (32-bit integer)
    - 20:22 Factor (8-bit integer)

    Returns a dict with a "LR9_Decode" list.
    """
    logger.debug(f'Decode_LR9 HEX : {hex_payload}')

    FrameType = hex_payload[0:2]
    UnitId = hex_payload[2:4]

    alarm_hex = hex_payload[4:8]
    extend_alarm_hex = hex_payload[8:12]

    Alarm = hex_to_bin(alarm_hex)
    ExtendAlarm = hex_to_bin(extend_alarm_hex)

    MeterIndex = hex_to_decimal(hex_payload[12:20])
    Factor = hex_to_decimal(hex_payload[20:22])


    # ใช้ mapping ตามไฟล์ Excel
    alarm_bits, alarm_active = decode_alarm_bits(alarm_hex, ALARM_BITS_DEFAULT_LR9)
    ext_alarm_bits, ext_alarm_active = decode_alarm_bits(extend_alarm_hex, EXT_ALARM_BITS_DEFAULT_LR9)

    result = {
        "LR9_Decode": [
            {
                "Hex": hex_payload,

                "FrameType": FrameType,
                "UnitID": UnitId,

                # ค่าเดิม
                "Alarm": Alarm,                     # binary string 16-bit
                "ExtendAlarm": ExtendAlarm,         # binary string 16-bit
                "MeterIndex": MeterIndex,
                "Factor": Factor,

                # เพิ่มเติมตามสเปกจากไฟล์
                "AlarmHex": alarm_hex,
                "ExtendAlarmHex": extend_alarm_hex,

                "AlarmBits": alarm_bits,
                "AlarmActiveList": alarm_active,

                "ExtendAlarmBits": ext_alarm_bits,
                "ExtendAlarmActiveList": ext_alarm_active,
                
                "DataSize": len(hex_payload) // 2,  # มีใน Excel (11)
            }
        ]
    }

    logger.debug(f'Return : {result}')
    return result

def decode_pulse3_keepalive(hex_payload: str) -> dict:
    """Decode a Pulse 3 keep-alive frame (prefix 0x30).

    The frame carries status and min/max flow values for the last 24h for
    channels A and B. The exact byte layout is derived from vendor docs.

    Raises:
        ValueError: if payload is shorter than the minimum required length.
    """
    payload = bytes.fromhex(hex_payload)
    if len(payload) < 11:
        raise ValueError("Payload too short for Pulse 3 keep-alive frame")

    # Byte[1] contains multiple status flags and a rolling frame counter (top bits)
    status_byte = payload[1]
    frame_counter = (status_byte >> 6) & 0b00000111

    # 3-byte big-endian integers for channel A/B last 24h max/min. Channel B min not present in this frame type (set to 0).
    ch_a_max = int.from_bytes(payload[2:5], byteorder='big')
    ch_a_min = int.from_bytes(payload[5:8], byteorder='big')
    ch_b_max = int.from_bytes(payload[8:11], byteorder='big') if len(payload) >= 11 else 0
    ch_b_min = 0

    result = {
        "HEX": hex_payload,
        "status": {
            "frameCounter": frame_counter,
            "hardwareError": bool(status_byte & 0b00010000),
            "lowBattery": bool(status_byte & 0b00001000),
            "configurationDone": bool(status_byte & 0b00000100),
            "configurationInconsistency": bool(status_byte & 0b00000010)
        },
        "channels": [
            {
                "name": "channel A",
                "flow": {"alarm": False, "last24hMin": ch_a_min, "last24hMax": ch_a_max},
                "tamperAlarm": False,
                "leakageAlarm": False
            },
            {
                "name": "channel B",
                "flow": {"alarm": False, "last24hMin": ch_b_min, "last24hMax": ch_b_max},
                "tamperAlarm": False,
                "leakageAlarm": False
            }
        ]
    }
    return result

def decode_hex_payload(hex_str: str) -> dict:
    """Decode a generic Adeunis-like 0x46 payload with two pulse counters.

    For 10-byte payloads (only first 10 bytes used if longer), interpret as:
    - byte[0]   frame type
    - byte[1]   status (opaque here)
    - byte[2:6] channel-1 pulse counter (uint32 BE)
    - byte[6:10] channel-2 pulse counter (uint32 BE)
    """
    payload = bytes.fromhex(hex_str)
    result = {"HEX": hex_str}

    if len(payload) < 10:
        logger.warning(f"Payload length {len(payload)} is less than expected 10 bytes.")

    result['frame_type'] = payload[0]
    result['status'] = payload[1]
    result['ch1_pulse_count'] = int.from_bytes(payload[2:6], byteorder='big')
    result['ch2_pulse_count'] = int.from_bytes(payload[6:10], byteorder='big')
    return result

# ----------------------- DB Operations -----------------------
def InsertDB(DeviceID, DateTimeVal, MeterIndex, Pressure, Flowrate, FrameType, config):
    """Insert a measurement row into the `standarddb` table.

    The query calculates `Consumption` as the delta between the new `MeterIndex`
    and the latest stored `MeterIndex` for the same device (subquery). This can
    be affected by out-of-order arrivals; consider ordering/queuing if exact
    monotonic deltas are required.
    """
    logger.info('Database Insert Start')

    DB_config = config.get("DB_config", {})
    if not DB_config:
        logger.error("Database configuration not found in config.json.")
        return

    host = DB_config.get("mysql_host", "localhost")
    port = DB_config.get("mysql_port", 3306)
    user = DB_config.get("mysql_user", "root")
    password = DB_config.get("mysql_Pass", "")
    database = DB_config.get("mysql_database", "dev")

    logger.debug(f'Connect to db.....')
    logger.debug(f'Host : {host}')
    logger.debug(f'User : {user}')
    logger.debug(f'Password : {password}')
    logger.debug(f'Database Name : {database}')
    logger.debug(f'Port : {port}')

    # Create a short-lived connection per call. For higher throughput,
    # consider using a pool (e.g., mysql.connector.pooling.MySQLConnectionPool).
    mysql_conn = mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=database
    )
    mysql_Cursor = mysql_conn.cursor()

    # Insert the latest reading and compute Consumption as difference from the
    # previous MeterIndex for the same DeviceID (if present).
    QueryInsertData = """
    INSERT INTO standarddb (`DeviceID`, `DateTime`, `MeterIndex`, `Pressure`, `Flowrate`, `Consumption`, `FrameType`, `updated_at`)
    VALUES (
        %s, %s, %s, %s, %s,
        %s - COALESCE((
            SELECT IFNULL(s.MeterIndex,0)
            FROM standarddb s
            WHERE s.DeviceID = %s
            ORDER BY `DateTime` DESC
            LIMIT 1
        ), 0),
        %s, NOW()
    )
    """
    mysql_Cursor.execute(QueryInsertData, (
        DeviceID, DateTimeVal, MeterIndex, Pressure, Flowrate,
        MeterIndex, DeviceID, FrameType
    ))
    mysql_conn.commit()

    if mysql_Cursor:
        mysql_Cursor.close()
    if mysql_conn:
        mysql_conn.close()

def CheckDeviceId(DeviceID, config) -> int:
    """Return 1 if a device exists (by `device_ref_id`), else 0.

    Queries the `device` table with COUNT(*). Any MySQL error is logged and
    treated as non-existing (returns 0) to avoid blocking ingestion.
    """
    logger.info('Database Check Device ID Start')

    DB_config = config.get("DB_config", {})
    if not DB_config:
        logger.error("Database configuration not found in config.json.")
        return 0

    host = DB_config.get("mysql_host", "localhost")
    port = DB_config.get("mysql_port", 3306)
    user = DB_config.get("mysql_user", "root")
    password = DB_config.get("mysql_Pass", "")
    database = DB_config.get("mysql_database", "dev")

    logger.debug(f'Connect to db.....')
    logger.debug(f'Host : {host}')
    logger.debug(f'User : {user}')
    logger.debug(f'Password : {password}')
    logger.debug(f'Database Name : {database}')

    result = None
    try:
        mysql_conn = mysql.connector.connect(
            host=host, user=user, port=port, password=password, database=database
        )
        mysql_Cursor = mysql_conn.cursor(dictionary=True)

        QueryCheckSensorId = "SELECT COUNT(*) AS count FROM device WHERE device_ref_id = %s"
        mysql_Cursor.execute(QueryCheckSensorId, (DeviceID,))
        result = mysql_Cursor.fetchone()
        logger.debug(f'Device Id : {DeviceID} Count = {result["count"] if result else "NULL"}')
    except mysql.connector.Error as e:
        logger.exception(f"MySQL error: {e}")
    finally:
        try:
            if mysql_Cursor:
                mysql_Cursor.close()
        except Exception:
            pass
        try:
            if mysql_conn:
                mysql_conn.close()
        except Exception:
            pass
    return result["count"] if result else 0

# ----------------------- Core message processing -----------------------
def process_message(clean_json: Dict[str, Any], config: dict) -> Dict[str, Any]:
    """Process a single uplink JSON message.

    Steps:
    1) Extract and base64-decode `data` to a hex string.
    2) Identify frame prefix to select the correct decoder.
    3) Optionally persist to DB if device exists (Adeunis / LR9 frames only).
    4) Persist full enriched message (raw + decoded) to disk under a frame-specific folder (or Fail).

    Returns a small status dict suitable for HTTP responses.
    """
    logger.info("HTTP message received.")
    logger.debug(f'Full message as JSON: {clean_json}')

    if 'data' not in clean_json:
        logger.warning("Missing 'data' key in incoming message. Skipping decode.")
        return {"status": "ok", "note": "no data field, skipped"}

    payload_b64 = clean_json.get('data')
    if not payload_b64:
        logger.warning("Empty 'data' field in message.")
        return {"status": "ok", "note": "empty data, skipped"}

    # Convert base64-encoded payload to hex string for convenient slicing
    payload_bytes = base64.b64decode(payload_b64)
    payload_hex = payload_bytes.hex()
    logger.debug(f"Base64 decoded to hex: {payload_hex}")

    # device & time
    device_info = clean_json.get('deviceInfo') or {}
    DeviceName = device_info.get('devEui') or device_info.get('deviceName')

    try:
    # Normalize incoming timestamp to Asia/Bangkok (+07:00) from nsTime; fallback to current time if parsing fails
        DateTimeVal = datetime.fromisoformat(clean_json['rxInfo'][0].get('nsTime')).astimezone(
            timezone(timedelta(hours=7))
        )
    except Exception:
        DateTimeVal = datetime.now(timezone(timedelta(hours=7)))

    frame_prefix = payload_hex[0:2].lower()

    if frame_prefix == "46":
    # Adeunis-like 0x46 frame: two channel counters (ch1/ch2). Saved under Adeunis or Fail.
        decoded_data = decode_hex_payload(payload_hex)
        clean_json['Data Decode'] = decoded_data

        if CheckDeviceId(DeviceName, config) == 1:
            logger.info(f"Have Sensor Id = {DeviceName}")
            MeterIndex = decoded_data.get('ch1_pulse_count', 0)
            Pressure = 0
            Flowrate = 0
            FrameType = payload_hex[0:2]

            try:
                logger.info('Insert into database (Adeunis frame)')
                InsertDB(DeviceName, DateTimeVal, MeterIndex, Pressure, Flowrate, FrameType, config)
                output_dir = 'Sensor-Data/Adeunis'
                os.makedirs(output_dir, exist_ok=True)
                filename = os.path.join(output_dir, f"adeunis_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(clean_json, f, indent=4, ensure_ascii=False)
                logger.debug(f'Drop File Name : {filename}')
            except Exception as e:
                logger.error(f'Insert : {e}')
                output_dir = 'Sensor-Data/Fail'
                os.makedirs(output_dir, exist_ok=True)
                filename = os.path.join(output_dir, f"adeunis_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(clean_json, f, indent=4, ensure_ascii=False)
                logger.debug(f'Drop File Name : {filename}')
        else:
            output_dir = 'Sensor-Data/Fail'
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.join(output_dir, f"adeunis_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(clean_json, f, indent=4, ensure_ascii=False)
            logger.debug(f'Drop File Name : {filename}')

        logger.info("Processed 0x46 frame.")
        return {"status": "ok", "frame": "46"}

    elif frame_prefix == "9d":
    # LR9 custom frame: alarms + meter index + factor. Saved under LR9 or Fail.
        payload_lr9 = decode_LR9(payload_hex)
        clean_json['Data Decode'] = payload_lr9

        if CheckDeviceId(DeviceName, config) == 1:
            logger.info(f"Have Sensor Id = {DeviceName}")
            MeterIndex = payload_lr9['LR9_Decode'][0].get('MeterIndex', 0)
            Pressure = 0
            Flowrate = 0
            FrameType = payload_hex[0:2]

            try:
                logger.info('Insert into database (LR9 frame)')
                InsertDB(DeviceName, DateTimeVal, MeterIndex, Pressure, Flowrate, FrameType, config)
                output_dir = 'Sensor-Data/LR9'
                os.makedirs(output_dir, exist_ok=True)
                filename = os.path.join(output_dir, f"LR9_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(clean_json, f, indent=4, ensure_ascii=False)
                logger.debug(f'Drop File Name : {filename}')
            except Exception as e:
                logger.error(f'Insert : {e}')
                output_dir = 'Sensor-Data/Fail'
                os.makedirs(output_dir, exist_ok=True)
                filename = os.path.join(output_dir, f"LR9_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(clean_json, f, indent=4, ensure_ascii=False)
                logger.debug(f'Drop File Name : {filename}')
        else:
            output_dir = 'Sensor-Data/Fail'
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.join(output_dir, f"LR9_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(clean_json, f, indent=4, ensure_ascii=False)
            logger.debug(f'Drop File Name : {filename}')

        logger.info("Processed 0x9d frame.")
        return {"status": "ok", "frame": "9d"}

    elif frame_prefix == "30":
    # Pulse 3 keep-alive frame: snapshot only (no DB persistence). Always stored under keepalive.
        clean_json['Data Decode'] = decode_pulse3_keepalive(payload_hex)
        output_dir = 'Sensor-Data/keepalive'
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, f"keepalive_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(clean_json, f, indent=4, ensure_ascii=False)
        logger.debug(f'Drop File Name : {filename}')
        logger.info("Processed 0x30 keepalive frame.")
        return {"status": "ok", "frame": "30"}

    else:
        logger.info('No Data Skip Message (unknown frame).')
        return {"status": "ok", "note": "unknown frame prefix", "prefix": frame_prefix}

# ----------------------- FastAPI App + lifespan -----------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Log application startup/shutdown events."""
    logger.info("Application startup.")
    try:
        yield
    finally:
        logger.info("Application shutdown.")

app = FastAPI(title=Program_Name, version=Program_Version, lifespan=lifespan)

# ----------- HTTP Basic Auth -----------
security = HTTPBasic()

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """
    Validate HTTP Basic auth against config values.

    Config keys used: Auth.username / Auth.password
    Returns the authenticated username or raises HTTP 401 on failure.
    """
    cfg_user = (config.get("Auth") or {}).get("username", "aurten")
    cfg_pass = (config.get("Auth") or {}).get("password", "aurten@2025")

    correct_username = secrets.compare_digest(credentials.username, cfg_user)
    correct_password = secrets.compare_digest(credentials.password, cfg_pass)

    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# Health endpoint does not require login
@app.get("/health")
def health():
    """Health probe endpoint (no auth required)."""
    return {"status": "ok", "service": Program_Name, "version": Program_Version}

# Ingestion endpoint requires login
@app.post("/ingest")
async def ingest(
    request: Request,
    _user: str = Depends(get_current_user)
):
    """Authenticated ingestion endpoint.

    Accepts a JSON body, delegates to `process_message`, and returns a concise
    status response. Errors are translated to proper HTTP codes.
    """
    try:
        body: Dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    try:
    # Offload to thread pool (DB I/O + file writes are blocking) so event loop remains responsive
        result = await run_in_threadpool(process_message, body, config)
        return JSONResponse(content=result)
    except Exception as e:
        logger.exception(f"Error decoding message: {e}")
        raise HTTPException(status_code=422, detail=str(e))

# ----------------------- Local dev / exe entry -----------------------
if __name__ == "__main__":
    API_Config = config.get("API_Listen", {})
    host = API_Config.get("host", "0.0.0.0")
    port = API_Config.get("port", 8000)

    logger.info(f"Starting {Program_Name} (Uvicorn) on http://{host}:{port}")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_config=None,   # Use current logging config (already bridged to Loguru via custom handler)
        access_log=True,    # Access logs routed through LoguruHandler
        workers=API_Config.get("workers", 1)
    )