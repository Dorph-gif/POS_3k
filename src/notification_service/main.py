from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uvicorn
from .database.database import create_database_service
import logging
from dotenv import load_dotenv
import os
import json
from aiokafka import AIOKafkaProducer
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import JSONResponse
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

load_dotenv()
SERVER_URL=os.getenv("SERVER_URL")

LOG_FILE = os.path.join("logs", "notification.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

app = FastAPI()

class LinkUpdated(BaseModel):
    link_id: int
    url: str
    last_update: str
    title: str = None
    user_name: str = None
    preview: str = None

kafka_producer = None
KAFKA_TOPIC_TO_SERVER = os.getenv("KAFKA_TOPIC_TO_SERVER")

@app.on_event("startup")
async def startup_event():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    await kafka_producer.start()
    logger.info("Kafka producer started")

@app.on_event("shutdown")
async def shutdown_event():
    await kafka_producer.stop()
    logger.info("Kafka producer stopped")

def get_links_from_database(link_id: int, offset: int, limit: int):
    db_service = create_database_service()
    return db_service.get_users_by_link_id(link_id, offset, limit)

async def send_with_kafka(update_notification: LinkUpdated):
    try:
        offset = 0
        limit = int(os.getenv("BATCH_SIZE"))
        while True:
            telegram_ids = get_links_from_database(update_notification.link_id, offset, limit)
            offset += limit
            
            async with httpx.AsyncClient() as client:
                for telegram_id in telegram_ids:
                            notification_data = {
                                "user_id": telegram_id,
                                "url": update_notification.url,
                                "last_update": update_notification.last_update,
                                "title": update_notification.title,
                                "user_name": update_notification.user_name,
                                "preview": update_notification.preview,
                            }
                            await kafka_producer.send_and_wait(KAFKA_TOPIC_TO_SERVER, json.dumps(notification_data).encode('utf-8'))
                            logger.info(f"Notification sent to Kafka topic {KAFKA_TOPIC_TO_SERVER} for link_id: {update_notification.link_id}")

    except Exception as e:
        logger.exception(f"Error processing link update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50))
RETRIES = int(os.getenv("RETRIES", 3))
TIMEOUT = float(os.getenv("HTTP_TIMEOUT", 3.0))
BACKOFF = float(os.getenv("BACKOFF_FACTOR", 0.3))

retry_strategy = Retry(
    total=RETRIES,
    backoff_factor=BACKOFF,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"]
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("http://", adapter)
session.mount("https://", adapter)

def send_with_http(update_notification: LinkUpdated) -> dict:
    try:
        offset = 0
        limit = BATCH_SIZE

        while True:
            telegram_ids = get_links_from_database(update_notification.link_id, offset, limit)
            if not telegram_ids:
                break
            offset += limit

            for telegram_id in telegram_ids:
                notification_data = {
                    "user_id": telegram_id,
                    "url": update_notification.url,
                    "last_update": update_notification.last_update,
                    "title": update_notification.title,
                    "user_name": update_notification.user_name,
                    "preview": update_notification.preview,
                }
                try:
                    response = session.post(
                        f"{SERVER_URL}/api/v1/updated/",
                        json=notification_data,
                        timeout=TIMEOUT
                    )
                    response.raise_for_status()
                    logger.info(f"Notification sent to {telegram_id}: {response.status_code}")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to send to {telegram_id}: {e}")

        return {"status": "ok", "message": "Notifications forwarded"}

    except Exception as e:
        logger.exception(f"Error processing link update: {e}")
        raise

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

app.add_middleware(BaseHTTPMiddleware, dispatch=limiter.middleware)

@app.exception_handler(RateLimitExceeded)
async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Too many requests, slow down!"},
    )

@app.post("/api/v1/link_updated")
@limiter.limit("5/minute")
async def link_updated(update_notification: LinkUpdated):
    logger.info(f"Received link update for link_id: {update_notification.link_id}, URL: {update_notification.url}")

    primary = os.getenv("MESSAGE_TRANSPORT", "HTTP").upper()
    secondary = "KAFKA" if primary == "HTTP" else "HTTP"

    try:
        if primary == "HTTP":
            return send_with_http(update_notification)
        else:
            return await send_with_kafka(update_notification)
    except Exception as primary_error:
        logger.warning(f"Primary transport '{primary}' failed: {primary_error}. Trying fallback '{secondary}'")

        try:
            if secondary == "HTTP":
                return send_with_http(update_notification)
            else:
                return await send_with_kafka(update_notification)
        except Exception as fallback_error:
            logger.error(f"Both transports failed: primary={primary_error}, fallback={fallback_error}")
            raise HTTPException(status_code=500, detail="Both transports failed to deliver notifications.")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
