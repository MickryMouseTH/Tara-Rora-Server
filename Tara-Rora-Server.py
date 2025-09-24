from datetime import datetime, timedelta, timezone
import mysql.connector
from loguru import logger
import json
import sys
import os
import base64
from typing import Any, Dict

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets

# NEW: lifespan
from contextlib import asynccontextmanager

# ----------------------- Configuration Values -----------------------
Program_Name = "Tara-Rora-Server"
Program_Version = "1.3"

default_config = {
    "DB_config": {
        "mysql_host": "",
        "mysql_port": 3306,
        "mysql_user": "",
        "mysql_Pass": "",
        "mysql_database": "",
    },
    "API_Listen": {
        "host": "0.0.0.0",
        "port": 8000
    },
    "Auth": {
        "username": "aurten",
        "password": "aurten@2025"
    },
    "log_Level": "DEBUG",
    "Log_Console_Enable": 1,
    "log_Backup": 90,
    "Log_Size": "10 MB"
}

# ----------------------- Load Configuration -----------------------
def Load_Config(default_config: dict, Program_Name: str) -> dict:
    config_file_path = f'{Program_Name}.config.json'
    if not os.path.exists(config_file_path):
        with open(config_file_path, 'w') as new_config_file:
            json.dump(default_config, new_config_file, indent=4)
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
    return config

# ----------------------- Loguru Logging Setup -----------------------
def Loguru_Logging(config: dict, Program_Name: str, Program_Version: str):
    logger.remove()

    log_Backup = int(config.get('log_Backup', 90))
    if log_Backup < 1:
        log_Backup = 1
    Log_Size = config.get('Log_Size', '10 MB')
    log_Level = config.get('log_Level', 'DEBUG').upper()

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_file_name = f'{Program_Name}_{Program_Version}.log'
    log_file = os.path.join(log_dir, log_file_name)

    if config.get('Log_Console_Enable', 0) == 1:
        logger.add(
            sys.stdout,
            level=log_Level,
            format="<green>{time}</green> | <blue>{level}</blue> | <cyan>{thread.id}</cyan> | <magenta>{function}</magenta> | {message}"
        )

    logger.add(
        log_file,
        format="{time} | {level} | {thread.id} | {function} | {message}",
        level=log_Level,
        rotation=Log_Size,
        retention=log_Backup,
        compression="zip"
    )

    logger.info('-' * 117)
    logger.info(f"Start {Program_Name} Version {Program_Version}")
    logger.info('-' * 117)

    return logger

# ----------------------- Helpers / Decoders -----------------------
def hex_to_bin(hex_str):
    return bin(int(hex_str, 16))[2:].zfill(len(hex_str) * 4)

def hex_to_decimal(hex_str):
    return int(hex_str, 16)

def decode_LR9(hex_payload: str) -> dict:
    logger.debug(f'Decode_LR9 HEX : {hex_payload}')
    result = {}
    FrameType = hex_payload[0:2]
    UnitId = hex_payload[2:4]
    Alarm = hex_to_bin(hex_payload[4:8])
    ExtendAlarm = hex_to_bin(hex_payload[8:12])
    MeterIndex = hex_to_decimal(hex_payload[12:20])
    Factor = hex_to_decimal(hex_payload[20:22])

    result["LR9_Decode"] = [
        {
            "Hex": hex_payload,
            "FrameType": FrameType,
            "UnitID": UnitId,
            "Alarm": Alarm,
            "ExtendAlarm": ExtendAlarm,
            "MeterIndex": MeterIndex,
            "Factor": Factor
        }
    ]
    logger.debug(f'Return : {result}')
    return result

def decode_pulse3_keepalive(hex_payload: str) -> dict:
    payload = bytes.fromhex(hex_payload)
    if len(payload) < 11:
        raise ValueError("Payload too short for Pulse 3 keep-alive frame")

    result = {}
    frame_type = payload[0]  # parsed but not used
    result["HEX"] = hex_payload

    status_byte = payload[1]
    frame_counter = (status_byte >> 6) & 0b00000111

    result["status"] = {
        "frameCounter": frame_counter,
        "hardwareError": bool(status_byte & 0b00010000),
        "lowBattery": bool(status_byte & 0b00001000),
        "configurationDone": bool(status_byte & 0b00000100),
        "configurationInconsistency": bool(status_byte & 0b00000010)
    }

    ch_a_max = int.from_bytes(payload[2:5], byteorder='big')
    ch_a_min = int.from_bytes(payload[5:8], byteorder='big')
    ch_b_max = int.from_bytes(payload[8:11], byteorder='big') if len(payload) >= 11 else 0
    ch_b_min = 0

    result["channels"] = [
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
    return result

def decode_hex_payload(hex_str: str) -> dict:
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

    mysql_conn = mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=database
    )
    mysql_Cursor = mysql_conn.cursor()

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
    logger.info("HTTP message received.")
    logger.debug(f'Full message as JSON: {clean_json}')

    if 'data' not in clean_json:
        logger.warning("Missing 'data' key in incoming message. Skipping decode.")
        return {"status": "ok", "note": "no data field, skipped"}

    payload_b64 = clean_json.get('data')
    if not payload_b64:
        logger.warning("Empty 'data' field in message.")
        return {"status": "ok", "note": "empty data, skipped"}

    payload_bytes = base64.b64decode(payload_b64)
    payload_hex = payload_bytes.hex()
    logger.debug(f"Base64 decoded to hex: {payload_hex}")

    # device & time
    device_info = clean_json.get('deviceInfo') or {}
    DeviceName = device_info.get('devEui') or device_info.get('deviceName')

    try:
        DateTimeVal = datetime.fromisoformat(clean_json['rxInfo'][0].get('nsTime')).astimezone(
            timezone(timedelta(hours=7))
        )
    except Exception:
        DateTimeVal = datetime.now(timezone(timedelta(hours=7)))

    frame_prefix = payload_hex[0:2].lower()

    if frame_prefix == "46":
        decoded_data = decode_hex_payload(payload_hex)
        clean_json['Data Decode'] = decoded_data

        if CheckDeviceId(DeviceName, config) == 1:
            logger.info(f"Have Sensor Id = {DeviceName}")
            MeterIndex = decoded_data.get('ch1_pulse_count', 0)
            Pressure = 0
            Flowrate = 0
            FrameType = payload_hex[0:2]

            try:
                logger.info('Insert Database.....')
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
        payload_lr9 = decode_LR9(payload_hex)
        clean_json['Data Decode'] = payload_lr9

        if CheckDeviceId(DeviceName, config) == 1:
            logger.info(f"Have Sensor Id = {DeviceName}")
            MeterIndex = payload_lr9['LR9_Decode'][0].get('MeterIndex', 0)
            Pressure = 0
            Flowrate = 0
            FrameType = payload_hex[0:2]

            try:
                logger.info('Insert Database.....')
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
config: dict = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global config
    try:
        config = Load_Config(default_config, Program_Name)
        Loguru_Logging(config, Program_Name, Program_Version)
        logger.info("Configuration loaded successfully.")
        yield
    finally:
        # place for graceful shutdown if needed
        pass

app = FastAPI(title=Program_Name, version=Program_Version, lifespan=lifespan)

# ----------- HTTP Basic Auth -----------
security = HTTPBasic()

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """
    ตรวจสอบ username/password จากไฟล์คอนฟิก (Auth.username / Auth.password)
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
# --------------------------------------

# health ไม่ต้องล็อกอิน
@app.get("/health")
def health():
    return {"status": "ok", "service": Program_Name, "version": Program_Version}

# ingest ต้องล็อกอิน
@app.post("/ingest")
async def ingest(
    request: Request,
    _user: str = Depends(get_current_user)
):
    try:
        body: Dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    try:
        result = process_message(body, config)
        return JSONResponse(content=result)
    except Exception as e:
        logger.exception(f"Error decoding message: {e}")
        raise HTTPException(status_code=422, detail=str(e))

# ----------------------- Local dev entry -----------------------
if __name__ == "__main__":
    import uvicorn
    # อ่าน host/port จากไฟล์คอนฟิก โดยไม่แตะ log (กันกรณียังไม่ init logger)
    try:
        cfg_for_run = Load_Config(default_config, Program_Name)
    except Exception:
        cfg_for_run = default_config

    host = (cfg_for_run.get("API_Listen") or {}).get("host", "0.0.0.0")
    port = (cfg_for_run.get("API_Listen") or {}).get("port", 8000)

    # เมื่อรันเป็นสคริปต์: ห้ามใช้ reload=True (จะเกิดคำเตือน)
    uvicorn.run(app, host=host, port=port)  # no reload
# ----------------------- End of File -----------------------