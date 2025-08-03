from datetime import datetime, timedelta, timezone
import concurrent.futures                # Import module for handling ThreadPoolExecutor (parallel execution)
import mysql.connector
from loguru import logger                # Import loguru for logging system
import time                              # Import time for delay functions (sleep)
import pika                              # Import pika for connecting and sending/receiving messages with RabbitMQ
import json                              # Import json for loading/saving config files in JSON format
import sys                               # Import sys for system functions and variables (e.g., stdout)
import os                                # Import os for operating system functions (e.g., creating folders)
import base64                            # Import base64 for decoding payload data


# ----------------------- Configuration Values -----------------------
Program_Name = "Rabbit-Recever-Server"  # Declare the program name (used for display and log file naming)
Program_Version = "1.1"          # Declare the program version (used in log file name)
# ---------------------------------------------------------------------

CPU_COUNT = os.cpu_count() or 1  # Check the number of CPU cores in the system (use 1 if unavailable)
executor = concurrent.futures.ThreadPoolExecutor(max_workers=CPU_COUNT)  # Create ThreadPoolExecutor for parallel tasks (not used in this file)

default_config = {
    "DB_config":{                           # MySQL configuration
        "mysql_host":"",                    # MySQL host address
        "mysql_port": 3306,                 # Default MySQL port
        "mysql_user":"",                    # MySQL username
        "mysql_Pass":"",                    # MySQL password
        "mysql_database":"",                # MySQL database name
    },
    "RabbitMQ_config":{                     # RabbitMQ configuration
        "mq_host": "",                      # RabbitMQ host address
        "mq_port": 5672,                    # RabbitMQ port (default is 5672)
        "mq_user": "",                      # RabbitMQ username
        "mq_pass": "",                      # RabbitMQ password
        "mq_virtual_host": "",              # RabbitMQ virtual host
        "mq_queue_name": "",                # Queue name to receive messages
        "retry_mq": 30,                     # Wait time (seconds) before retrying MQ connection
    },
    "log_Level": "DEBUG",                   # Log level
    "Log_Console_Enable": 1,                # Display log in console or not (1=show, 0=do not show)
    "log_Backup": 90,                       # Number of days to keep logs before deleting
    "Log_Size": "10 MB"                     # Maximum log file size per file
}
# ----------------------- Load Configuration -----------------------
def Load_Config(default_config, Program_Name):
    """
    Load configuration from JSON file. If the file does not exist, create it with default values.
    """
    config_file_path = f'{Program_Name}.config.json'

    # Create config file with default values if it does not exist.
    if not os.path.exists(config_file_path):
        default_config = default_config 
        with open(config_file_path, 'w') as new_config_file:
            json.dump(default_config, new_config_file, indent=4)

    # Load configuration from file
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
    
    return config

# ----------------------- Loguru Logging Setup -----------------------
def Loguru_Logging(config, Program_Name, Program_Version):
    """
    Set up Loguru logging based on configuration.
    """
    logger.remove()

    log_Backup = int(config.get('log_Backup', 90))  # Default to 90 days if not set
    if log_Backup < 1:  # Ensure log retention is at least 1 day
        log_Backup = 1
    Log_Size = config.get('Log_Size', '10 MB')  # Default to 10 MB if not set
    log_Level = config.get('log_Level', 'DEBUG').upper()  # Default to DEBUG if not set

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_file_name = f'{Program_Name}_{Program_Version}.log'
    log_file = os.path.join(log_dir, log_file_name)

    # Enable console logging if configured
    if config.get('Log_Console_Enable', 0) == 1:
        logger.add(
            sys.stdout, 
            level=log_Level, 
            format="<green>{time}</green> | <blue>{level}</blue> | <cyan>{thread.id}</cyan> | <magenta>{function}</magenta> | {message}"
        )

    # Add file logging with rotation and retention
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

def callback(ch, method, properties, body):
    """
    Callback function that is called when a message is received from RabbitMQ.
    - Converts payload from base64 to bytes, bytes to hex, decodes hex payload.
    - Adds decoded data to json, writes to file, and acknowledges the message.
    - Logs information and all steps.
    """
    logger.info("Message received from MQ.")  # Log message receipt
    logger.debug(f"Raw body: {body}")        # Log raw body data

    try:
        message = body.decode('utf-8')  # Decode bytes to string (JSON)
        logger.debug(f'Message decoded to string: {message}')  # Log string message

        clean_json = json.loads(message)  # Convert string to dict
        logger.debug(f'Parsed JSON keys: {list(clean_json.keys())}')  # Log keys in JSON
        logger.debug(f'Full message as JSON: {clean_json}')          # Log full JSON message

        if 'data' not in clean_json:  # If 'data' key is missing, skip decoding
            logger.warning("Missing 'data' key in incoming message. Skipping decode.")
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Immediately acknowledge message
            return

        logger.debug("Found 'data' key, proceeding to decode.")  # Log found data key
        
        payload_b64 = clean_json.get('data')  # Retrieve base64 payload
        logger.debug(f"Base64 payload: {payload_b64}")  # Log base64 payload
        if not payload_b64:  # If there is no data in 'data' field
            logger.warning("No 'data' field in message.")
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Immediately acknowledge message
            return

        payload_bytes = base64.b64decode(payload_b64)  # Decode base64 to bytes
        logger.debug(f"Base64 decoded bytes: {payload_bytes}")  # Log decoded bytes
        payload_hex = payload_bytes.hex()  # Convert bytes to hex string
        logger.debug(f"Base64 decoded to hex: {payload_hex}")  # Log hex string
        
        
        DeviceName = clean_json['deviceInfo'].get('devEui')
        DateTime = datetime.fromisoformat(clean_json['rxInfo'][0].get('nsTime')).astimezone(timezone(timedelta(hours=7)))

        if(payload_hex[0:2] == "46"):
            decoded_data = decode_hex_payload(payload_hex)  # Decode hex payload
            clean_json['Data Decode'] = decoded_data  # Add decoded data to json
            logger.debug(f'Augmented JSON (with decoded data): {clean_json}')  # Log new json
            if(CheckDeviceId(DeviceName) == 1):
                logger.info(f'Have Senser Id = {clean_json['deviceInfo'].get('devEui')}')

                MeterIndex = clean_json['Data Decode'].get('ch1_pulse_count')
                Pressure = 0
                Flowrate = 0
                FrameType = payload_hex[0:2]

                try:
                    logger.info(f'Insert Database.....')
                    logger.debug(f'DeviceName = {DeviceName}')
                    logger.debug(f'DateTime = {DateTime}')
                    logger.debug(f'MeterIndex = {MeterIndex}')
                    logger.debug(f'Pressure = {Pressure}')
                    logger.debug(f'Flowrate = {Flowrate}')
                    logger.debug(f'FrameType = {FrameType}')
                    InsertDB(DeviceName,DateTime,MeterIndex,Pressure,Flowrate,FrameType)
                    output_dir = 'Sensor-Data/Adeunis'  # Folder for output files
                    os.makedirs(output_dir, exist_ok=True)  # Create folder if it does not exist
                    filename = os.path.join(output_dir, f"adeunis_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")  # Set output file name
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(clean_json, f, indent=4, ensure_ascii=False)  # Write json to file
                        logger.debug(f'Drop File Name : {filename}')  # Log file name
                except Exception as e:
                    logger.error(f'Insert : {e}')
                    output_dir = 'Sensor-Data/Fail'  # Folder for output files
                    os.makedirs(output_dir, exist_ok=True)  # Create folder if it does not exist
                    filename = os.path.join(output_dir, f"adeunis_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")  # Set output file name
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(clean_json, f, indent=4, ensure_ascii=False)  # Write json to file
                        logger.debug(f'Drop File Name : {filename}')  # Log file name
            else:
                output_dir = 'Sensor-Data/Fail'  # Folder for output files
                os.makedirs(output_dir, exist_ok=True)  # Create folder if it does not exist
                filename = os.path.join(output_dir, f"adeunis_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")  # Set output file name
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(clean_json, f, indent=4, ensure_ascii=False)  # Write json to file
                    logger.debug(f'Drop File Name : {filename}')  # Log file name
            logger.info("Successfully wrote decoded data to file.")  # Log success

        elif(payload_hex[0:2] == "9d"):
            Payload_LR9_Decode = decode_LR9(payload_hex)
            clean_json['Data Decode'] = Payload_LR9_Decode
            if(CheckDeviceId(DeviceName) == 1):
                logger.info(f'Have Senser Id = {clean_json['deviceInfo'].get('deviceName')}')

                MeterIndex = Payload_LR9_Decode['LR9_Decode'][0].get('MeterIndex')
                Pressure = 0
                Flowrate = 0
                FrameType = payload_hex[0:2]

                try:
                    logger.info(f'Insert Database.....')
                    logger.debug(f'DeviceName = {DeviceName}')
                    logger.debug(f'DateTime = {DateTime}')
                    logger.debug(f'MeterIndex = {MeterIndex}')
                    logger.debug(f'Pressure = {Pressure}')
                    logger.debug(f'Flowrate = {Flowrate}')
                    logger.debug(f'FrameType = {FrameType}')
                    InsertDB(DeviceName,DateTime,MeterIndex,Pressure,Flowrate,FrameType)
                    output_dir = 'Sensor-Data/LR9'  # Folder for output files
                    os.makedirs(output_dir, exist_ok=True)  # Create folder if it does not exist
                    filename = os.path.join(output_dir, f"LR9_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")  # Set output file name
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(clean_json, f, indent=4, ensure_ascii=False)  # Write json to file
                        logger.debug(f'Drop File Name : {filename}')  # Log file name
                except Exception as e:
                    logger.error(f'Insert : {e}')
                    output_dir = 'Sensor-Data/Fail'  # Folder for output files
                    os.makedirs(output_dir, exist_ok=True)  # Create folder if it does not exist
                    filename = os.path.join(output_dir, f"LR9_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")  # Set output file name
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(clean_json, f, indent=4, ensure_ascii=False)  # Write json to file
                        logger.debug(f'Drop File Name : {filename}')  # Log file name

            else:
                output_dir = 'Sensor-Data/Fail'  # Folder for output files
                os.makedirs(output_dir, exist_ok=True)  # Create folder if it does not exist
                filename = os.path.join(output_dir, f"LR9_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")  # Set output file name
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(clean_json, f, indent=4, ensure_ascii=False)  # Write json to file
                    logger.debug(f'Drop File Name : {filename}')  # Log file name
            logger.info("Successfully wrote decoded data to file.")  # Log success
        
        elif(payload_hex[0:2] == "30"):
            clean_json['Data Decode'] = decode_pulse3_keepalive(payload_hex)
            output_dir = 'Sensor-Data/keepalive'  # Folder for output files
            os.makedirs(output_dir, exist_ok=True)  # Create folder if it does not exist
            filename = os.path.join(output_dir, f"keepalive_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json")  # Set output file name
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(clean_json, f, indent=4, ensure_ascii=False)  # Write json to file
                logger.debug(f'Drop File Name : {filename}')  # Log file name
            logger.info("Successfully wrote decoded data to file.")  # Log success
            
        else:
            logger.info('No Data Skip Message')


    except Exception as e:
        logger.exception(f"Error decoding message: {e}")  # Log error
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  # Nack and requeue message
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message after successful processing
    logger.info(f'Acknowledge the message to remove it from the queue.')  # Log acknowledge

def InsertDB(DeviceID, DateTime, MeterIndex, Pressure, Flowrate, FrameType):
    logger.info('Database Insert Start')
    
    DB_config = config.get("DB_config", {})
    if not DB_config:
        logger.error("Database configuration not found in config.json.")
        return
    host = DB_config.get("mysql_host", "localhost")
    port = DB_config.get("mysql_port", 3306)  # Default MySQL port is 3306
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
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
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
    # หมายเหตุ: ส่ง MeterIndex และ DeviceID ซ้ำอีกตัว สำหรับใช้ใน subquery
    mysql_Cursor.execute(QueryInsertData, (
        DeviceID, DateTime, MeterIndex, Pressure, Flowrate,
        MeterIndex, DeviceID, FrameType
    ))
    mysql_conn.commit()

    if mysql_Cursor:
        mysql_Cursor.close()
    if mysql_conn:
        mysql_conn.close()

def CheckDeviceId(DeviceID):
    logger.info('Database Check Device ID Start')
    
    DB_config = config.get("DB_config", {})
    if not DB_config:
        logger.error("Database configuration not found in config.json.")
        return 0 # Return 0 if no config found
    host = DB_config.get("mysql_host", "localhost")
    port = DB_config.get("mysql_port", 3306)  # Default MySQL port is 3306
    user = DB_config.get("mysql_user", "root")
    password = DB_config.get("mysql_Pass", "")
    database = DB_config.get("mysql_database", "dev")

    logger.debug(f'Connect to db.....')
    logger.debug(f'Host : {host}')
    logger.debug(f'User : {user}')
    logger.debug(f'Password : {password}')
    logger.debug(f'Database Name : {database}')

    try:
        mysql_conn = mysql.connector.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            database=database
        )
        mysql_Cursor = mysql_conn.cursor(dictionary=True)
        
        QueryCheckSensorId = f"SELECT COUNT(*) AS count FROM device WHERE device_ref_id = %s"
        mysql_Cursor.execute(QueryCheckSensorId, (DeviceID,))
        result = mysql_Cursor.fetchone()  # <-- fetch result here

        logger.debug(f'Device Id : {DeviceID} Count = {result["count"]}')
    except mysql.connector.Error as e:
        logger.exception(f"MySQL error: {e}")
    finally:
        if mysql_Cursor:
            mysql_Cursor.close()
        if mysql_conn:
            mysql_conn.close()
    return result["count"] if result else 0

def hex_to_bin(hex_str):
    return bin(int(hex_str, 16))[2:].zfill(len(hex_str) * 4)

def hex_to_decimal(hex_str):
    return int(hex_str, 16)

def decode_LR9(hex_payload: str) -> dict:
    logger.debug(f'Decode_LR9 HEX : {hex_payload}')
    result = {}
    FrameType = hex_payload[0:2]
    logger.debug(f'FrameType HEX : {FrameType}')
    UnitId = hex_payload[2:4]
    logger.debug(f'UnitId HEX : {UnitId}')
    Alarm = hex_to_bin(hex_payload[4:8])
    logger.debug(f'Alarm HEX : {hex_payload[4:8]}')
    ExtendAlarm = hex_to_bin(hex_payload[8:12])
    logger.debug(f'ExtendAlarm HEX : {hex_payload[8:12]}')
    MeterIndex = hex_to_decimal(hex_payload[12:20])
    logger.debug(f'MeterIndex HEX : {hex_payload[12:20]}')
    logger.debug(f'MeterIndex DEC : {MeterIndex}')
    Factor = hex_to_decimal(hex_payload[20:22])
    logger.debug(f'Factor HEX : {hex_payload[20:22]}')
    logger.debug(f'Factor DEC : {Factor}')

    result["LR9_Decode"] = [
        {
            "Hex" : hex_payload,
            "FrameType": FrameType,
            "UnitID" : UnitId,
            "Alarm" : Alarm,
            "ExtendAlarm" : ExtendAlarm,
            "MeterIndex" : MeterIndex,
            "Factor" : Factor
        }
    ]
    logger.debug(f'Retune : {result}')

    return result

def decode_pulse3_keepalive(hex_payload: str) -> dict:
    """
    Decode Adeunis Pulse 3 Keep Alive frame (0x30).
    
    Args:
        hex_payload (str): Hexadecimal payload string (e.g., "30800003E8000003E70000")
    
    Returns:
        dict: Decoded JSON structure
    """
    payload = bytes.fromhex(hex_payload)
    
    if len(payload) < 11:
        raise ValueError("Payload too short for Pulse 3 keep-alive frame")

    result = {}

    # Frame type
    frame_type = payload[0]
    result["HEX"] = hex_payload

    # Status byte
    status_byte = payload[1]
    frame_counter = (status_byte >> 6) & 0b00000111  # Bits 7-6-5

    result["status"] = {
        "frameCounter": frame_counter,
        "hardwareError": bool(status_byte & 0b00010000),
        "lowBattery": bool(status_byte & 0b00001000),
        "configurationDone": bool(status_byte & 0b00000100),
        "configurationInconsistency": bool(status_byte & 0b00000010)
    }

    # Channel A (3 bytes each, big-endian)
    ch_a_max = int.from_bytes(payload[2:5], byteorder='big')
    ch_a_min = int.from_bytes(payload[5:8], byteorder='big')

    # Channel B (optional, check length)
    ch_b_max = int.from_bytes(payload[8:11], byteorder='big') if len(payload) >= 11 else 0
    ch_b_min = 0  # ไม่มีข้อมูล min เพิ่มเติมในเฟรมนี้

    result["channels"] = [
        {
            "name": "channel A",
            "flow": {
                "alarm": False,  # ไม่พบ bit แจ้ง alarm ในเฟรมนี้
                "last24hMin": ch_a_min,
                "last24hMax": ch_a_max
            },
            "tamperAlarm": False,
            "leakageAlarm": False
        },
        {
            "name": "channel B",
            "flow": {
                "alarm": False,
                "last24hMin": ch_b_min,
                "last24hMax": ch_b_max
            },
            "tamperAlarm": False,
            "leakageAlarm": False
        }
    ]

    return result

def decode_hex_payload(hex_str):
    """
    Function to decode payload data in hex string format.
    - Converts hex to bytes.
    - Extracts various fields (frame_type, status, pulse count).
    - Logs each step.
    - Returns a dict containing decoded data.
    """
    payload = bytes.fromhex(hex_str)  # Convert hex string to bytes
    logger.debug(f"Payload bytes from hex: {payload}")  # Log bytes

    result = {}  # Dict for storing results

    result['HEX'] = hex_str  # Store hex string
    logger.debug(f"Stored HEX string: {result['HEX']}")  # Log hex string

    if len(payload) < 10:  # Check payload length
        logger.warning(f"Payload length {len(payload)} is less than expected 10 bytes.")  # Log warning

    result['frame_type'] = payload[0]  # First byte is frame_type
    logger.debug(f"frame_type: {result['frame_type']}")  # Log frame_type

    result['status'] = payload[1]      # Second byte is status
    logger.debug(f"status: {result['status']}")  # Log status

    # Bytes 2-5 (4 bytes) are ch1_pulse_count
    result['ch1_pulse_count'] = int.from_bytes(payload[2:6], byteorder='big')
    logger.debug(f"ch1_pulse_count: {result['ch1_pulse_count']}")  # Log ch1_pulse_count

    # Bytes 6-9 (4 bytes) are ch2_pulse_count
    result['ch2_pulse_count'] = int.from_bytes(payload[6:10], byteorder='big')
    logger.debug(f"ch2_pulse_count: {result['ch2_pulse_count']}")  # Log ch2_pulse_count

    return result  # Return result dict

global config  # Declare global config variable to be used in multiple functions
config = None  # Initialize config variable

def main():
    """
    Main function
    - Configures logging
    - Connects to RabbitMQ and starts consuming messages
    - Contains a loop for automatic reconnection on error
    - Supports KeyboardInterrupt to stop the program
    """
    try:
        config = Load_Config(default_config, Program_Name)  # Load configuration
        logger = Loguru_Logging(config, Program_Name, Program_Version)  # Set up logging
        logger.info("Configuration loaded successfully.")  # Log successful config load
    except FileNotFoundError:
        print("Configuration file not found. Please ensure 'config.json' exists.", file=sys.stderr)
        sys.exit(1)  # Exit if config file not found
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON configuration: {e}", file=sys.stderr)
        sys.exit(1)  # Exit if JSON decode error
    except KeyError as e:
        print(f"Missing key in configuration: {e}", file=sys.stderr)
        sys.exit(1)  # Exit if key error in config
    except ValueError as e:
        print(f"Invalid value in configuration: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error loading configuration or setting up logging: {e}", file=sys.stderr)
        sys.exit(1)  # Exit if configuration or logging setup fails

    logger.info("Starting main process for RabbitMQ consumer.")  # Log main start
    connection = None  # Variable to store connection object
    while True:  # Loop to reconnect/consume messages again when an error occurs
        try:
            RabbitMQ_config = config.get("RabbitMQ_config", {})  # Get RabbitMQ config
            if not RabbitMQ_config:
                logger.error("RabbitMQ configuration not found in config.json.")
                return  # Exit if no RabbitMQ config found
            logger.debug(f"Connecting to MQ at {RabbitMQ_config.get('mq_host',"")} with user {RabbitMQ_config.get('mq_user',"")}")  # Log connection info
            credentials = pika.PlainCredentials(RabbitMQ_config.get('mq_user',""), RabbitMQ_config.get('mq_pass',''))  # Create credentials
            connection_params = pika.ConnectionParameters(
                host=RabbitMQ_config.get('mq_host', ''),  # RabbitMQ host
                port=RabbitMQ_config.get('mq_port', 5672),  # RabbitMQ port (default 5672)   
                virtual_host=RabbitMQ_config.get('mq_virtual_host', '/'),  # RabbitMQ virtual host
                credentials=credentials
            )  # Create connection parameters for RabbitMQ
            connection = pika.BlockingConnection(connection_params)  # Connect to RabbitMQ
            channel = connection.channel()  # Create channel for RabbitMQ
            logger.info("Connected to RabbitMQ successfully.")  # Log successful connection

            queue_name = RabbitMQ_config.get('mq_queue_name',"")  # Queue name to consume
            logger.debug(f"Queue declare params: name={queue_name}, durable=True")  # Log queue info
            channel.queue_declare(queue=queue_name, durable=True)  # Declare queue (if not exists)
            logger.info(f"Queue '{queue_name}' declared successfully.")  # Log successful queue declaration

            # Start consuming messages from the queue using callback
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,  # Callback function to be called when a message arrives
                auto_ack=False                 # Do not auto-acknowledge (must call ch.basic_ack manually)
            )
            logger.info("Started consuming messages from RabbitMQ.")  # Log start consuming
            channel.start_consuming()  # Start consuming messages (blocking)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Stopping consumption...")  # Log Ctrl+C
            try:
                channel.stop_consuming()  # Stop consuming
            except Exception as e:
                logger.error(f"Error stopping consumption: {e}")  # Log error stopping consumption
            break
        except Exception as e:
            logger.exception(f"An error occurred with RabbitMQ connection: {e}. Reconnecting in {int(RabbitMQ_config.get('retry_mq',30))} seconds...")  # Log error
            if connection is not None:
                try:
                    connection.close()  # Close connection if exists
                except Exception as e_close:
                    logger.error(f"Error closing RabbitMQ connection: {e_close}")  # Log error closing connection
            time.sleep(int(RabbitMQ_config.get('retry_mq',30)))  # Wait before reconnecting

if __name__ == '__main__':  # Program entry point
    main()  # Call main