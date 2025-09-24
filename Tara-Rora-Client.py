import json
import os
import sys
import time
from typing import Optional, Dict, Any

import pika
import requests
from loguru import logger

PROGRAM_NAME = "Tara-Rora-Client"
PROGRAM_VERSION = "1.0"

DEFAULT_CONFIG = {
    "RabbitMQ": {
        "host": "localhost",
        "port": 5672,
        "username": "guest",
        "password": "guest",
        "virtual_host": "/",
        "queue_name": "sensor.ingest",
        "prefetch": 1,
        "connection_retry_sec": 5
    },
    "HTTP_Target": {
        "url": "http://127.0.0.1:8000/ingest",
        "basic_user": "aurten",
        "basic_pass": "aurten@2025",
        "timeout_sec": 10,
        "verify_ssl": True
    },
    "Logging": {
        "level": "INFO",
        "console": 1,
        "file_size": "10 MB",
        "backup_days": 30
    }
}

def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, indent=4)
        print(f"[INFO] Created default config at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def setup_logger(cfg: Dict[str, Any]):
    logger.remove()
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{PROGRAM_NAME}_{PROGRAM_VERSION}.log")

    level = (cfg.get("Logging") or {}).get("level", "INFO").upper()
    console = (cfg.get("Logging") or {}).get("console", 1)
    file_size = (cfg.get("Logging") or {}).get("file_size", "10 MB")
    backup_days = int((cfg.get("Logging") or {}).get("backup_days", 30))

    if console:
        logger.add(sys.stdout, level=level,
                   format="<green>{time}</green> | <blue>{level}</blue> | <cyan>{thread.id}</cyan> | <magenta>{function}</magenta> | {message}")
    logger.add(log_file, level=level,
               rotation=file_size, retention=backup_days, compression="zip",
               format="{time} | {level} | {thread.id} | {function} | {message}")

    logger.info("-" * 96)
    logger.info(f"Start {PROGRAM_NAME} v{PROGRAM_VERSION}")
    logger.info("-" * 96)

class RabbitToHttpForwarder:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.mq = cfg["RabbitMQ"]
        self.http = cfg["HTTP_Target"]
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

    def connect(self):
        creds = pika.PlainCredentials(self.mq["username"], self.mq["password"])
        params = pika.ConnectionParameters(
            host=self.mq["host"],
            port=self.mq["port"],
            virtual_host=self.mq.get("virtual_host", "/"),
            credentials=creds,
            heartbeat=30,
            blocked_connection_timeout=60,
            client_properties={"connection_name": f"{PROGRAM_NAME}-{os.getpid()}"}
        )
        while True:
            try:
                logger.info(f"Connecting RabbitMQ {self.mq['host']}:{self.mq['port']} vhost={self.mq.get('virtual_host','/')}")
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=self.mq.get("prefetch", 1))
                logger.info("RabbitMQ connected.")
                return
            except Exception as e:
                logger.exception(f"RabbitMQ connect failed: {e}")
                time.sleep(self.mq.get("connection_retry_sec", 5))

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass

    def post_once(self, payload: Dict[str, Any]) -> int:
        """ส่งไปยังเซิร์ฟเวอร์หนึ่งครั้ง — คืนค่า HTTP status code (หรือโยน exception ถ้าเชื่อมต่อไม่ได้)"""
        url = self.http["url"]
        auth = (self.http["basic_user"], self.http["basic_pass"])
        timeout = self.http.get("timeout_sec", 10)
        verify = self.http.get("verify_ssl", True)
        resp = requests.post(url, json=payload, auth=auth, timeout=timeout, verify=verify)
        return resp.status_code

    def on_message(self, ch, method, properties, body: bytes):
        queue = self.mq["queue_name"]
        logger.info(f"Received delivery_tag={method.delivery_tag} from '{queue}'")
        # พยายาม parse JSON
        try:
            msg = json.loads(body.decode("utf-8"))
        except Exception as e:
            # ข้อมูลเสีย ห้ามเคลียร์ออกจากคิวตาม requirement → เราจะ requeue
            logger.error(f"Invalid JSON: {e}. Requeue.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        # ส่งไปเซิร์ฟเวอร์
        try:
            status = self.post_once(msg)
            logger.info(f"POST -> {self.http['url']} status={status}")

            if 200 <= status < 300:
                # สำเร็จเท่านั้น จึง ack (ลบออกจากคิว)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.debug("ACK (delivered).")
            else:
                # ไม่ใช่ 2xx → ห้ามเคลียร์คิว → ใส่กลับคิวทันที
                logger.warning(f"Non-2xx ({status}). NACK requeue.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            # เชื่อมไม่ได้ → รอ 30 วิ แล้วค่อยลองใหม่ (โดย requeue)
            logger.warning(f"Server unreachable: {e}. Wait 30s then requeue.")
            time.sleep(30)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except Exception as e:
            # ความผิดพลาดอื่นๆ ที่ไม่แน่ใจ → ป้องกันเคลียร์คิว → requeue
            logger.exception(f"Unexpected error: {e}. NACK requeue.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def consume_forever(self):
        q = self.mq["queue_name"]
        while True:
            try:
                if not self.connection or self.connection.is_closed:
                    self.connect()
                self.channel.queue_declare(queue=q, durable=True)
                logger.info(f"Consuming '{q}' prefetch={self.mq.get('prefetch',1)}")
                self.channel.basic_consume(queue=q, on_message_callback=self.on_message, auto_ack=False)
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                logger.exception(f"AMQP connection error: {e}")
                self.close()
                time.sleep(self.mq.get("connection_retry_sec", 5))
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt: stop.")
                self.close()
                break
            except Exception as e:
                logger.exception(f"Consume error: {e}")
                self.close()
                time.sleep(self.mq.get("connection_retry_sec", 5))

def main():
    cfg_path = f"{PROGRAM_NAME}.config.json"
    cfg = load_config(cfg_path)
    setup_logger(cfg)
    fwd = RabbitToHttpForwarder(cfg)
    fwd.consume_forever()

if __name__ == "__main__":
    main()
