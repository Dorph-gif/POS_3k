from typing import List, Optional

import logging
import os
import json
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import uvicorn
import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from telethon import TelegramClient
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from src.server.scrapper_client import ScrapperClient
from src.server.telegram_client import TelegramClientManager

load_dotenv()

LOG_FILE = os.path.join("logs", "server.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

class SubscriptionRequest(BaseModel):
    url: str
    tags: Optional[List[str]] = None
    filters: Optional[List[str]] = None
    user_id: int

class UpdateNotification(BaseModel):
    user_id: int
    url: str
    last_update: str
    title: str = None
    user_name: str = None
    preview: str = None

async def get_scrapper_client() -> ScrapperClient:
    scrapper_url = os.getenv("SCRAPPER_URL")
    if not scrapper_url:
        raise HTTPException(status_code=500, detail="SCRAPPER_URL environment variable not set.")
    return ScrapperClient(base_url=scrapper_url)

kafka_consumer = None
KAFKA_TOPIC_TO_SERVER = os.getenv("KAFKA_TOPIC_TO_SERVER")

kafka_dlq_consumer = None
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dead-letter-topic")

async def check_for_updates():
    while True:
        await asyncio.sleep(60)
        try:
            async for msg in kafka_consumer:
                update_notification = json.loads(msg.value.decode('utf-8'))
                logger.info(f"Received message from Kafka: {update_notification}")
                update_notification = UpdateNotification(**update_notification)

                message = f"Обновление: {update_notification.title}\n"
                message += f"User: {update_notification.user_name}\n"
                message += f"Date: {update_notification.last_update}\n"
                message += f"Preview: {update_notification.preview[:200]}...\n"

                await app.tg_client.send_message(update_notification.user_id, message)
                logger.info(f"Sent notification to user {update_notification.user_id} for URL {update_notification.url}")
        except Exception as e:
            logger.error(f"Error processing messages from Kafka: {e}")
            if kafka_dlq_producer and 'msg' in locals():
                try:
                    await kafka_dlq_producer.send_and_wait(
                        topic=KAFKA_DLQ_TOPIC,
                        value=msg.value
                    )
                    logger.warning(f"Message отправлено в DLQ: {msg.value.decode('utf-8')}")
                except Exception as dlq_error:
                    logger.error(f"Ошибка при отправке в DLQ: {dlq_error}")

async def process_dead_letter_queue():
    while True:
        try:
            async for msg in kafka_dlq_consumer:
                value = msg.value.decode("utf-8")
                logger.warning(f"[DLQ] Получено сообщение: {value}")
        except Exception as e:
            logger.error(f"[DLQ] Ошибка при обработке сообщения: {e}")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    tg_client_manager = TelegramClientManager(
        session_name="fastapi_bot_session",
        api_id=int(os.getenv("BOT_API_ID")),
        api_hash=os.getenv("BOT_API_HASH"),
        bot_token=os.getenv("BOT_TOKEN"),
    )
    global kafka_consumer
    kafka_consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_TO_SERVER,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="server-group"
    )
    global kafka_dlq_consumer
    kafka_dlq_consumer = AIOKafkaConsumer(
        KAFKA_DLQ_TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="server-dlq-group"
    )
    global kafka_dlq_producer
    kafka_dlq_producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    )
    try:
        await kafka_consumer.start()
        await kafka_dlq_consumer.start()
        await kafka_dlq_producer.start()

        asyncio.create_task(check_for_updates())
        asyncio.create_task(process_dead_letter_queue())

        app.tg_client = tg_client_manager
        await tg_client_manager.__aenter__()
        yield
    finally:
        await kafka_dlq_consumer.stop()
        await kafka_dlq_producer.stop()
        await kafka_consumer.stop()
        await tg_client_manager.__aexit__(None, None, None)

app = FastAPI(lifespan=lifespan)

async def get_telegram_client(app: FastAPI = Depends()) -> TelegramClientManager:
    return app.tg_client

@app.post("/api/v1/subscriptions/")
async def create_subscription(
    subscription: SubscriptionRequest,
    scrapper_client: ScrapperClient = Depends(get_scrapper_client)
):
    try:
        result = await asyncio.to_thread(
            scrapper_client.create_subscription,
            user_id=subscription.user_id,
            url=subscription.url,
            tags=subscription.tags,
            filters=subscription.filters,
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to create subscription: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/v1/subscriptions/{user_id}")
async def delete_subscription(
    user_id: int,
    url: str,
    scrapper_client: ScrapperClient = Depends(get_scrapper_client)
):
    try:
        logger.info(f"Deleting subscription with {url} to user {user_id}")
        result = await asyncio.to_thread(scrapper_client.delete_subscription, user_id=user_id, url=url)
        return
    except Exception as e:
        logger.exception("Failed to delete subscription")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/subscriptions/{user_id}")
async def list_subscriptions(
    user_id: int,
    scrapper_client: ScrapperClient = Depends(get_scrapper_client)
):
    try:
        logger.info(f"Listing subscription for user {user_id}")
        result = await asyncio.to_thread(scrapper_client.get_subscriptions, user_id=user_id)
        return result
    except Exception as e:
        logger.exception("Failed to list subscriptions")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/updated/")
async def send_update_notification(
    update_notification: UpdateNotification,
):
    try:
        telegram_client = get_telegram_client()

        message = f"Обновление: {update_notification.title}\n"
        message += f"User: {update_notification.user_name}\n"
        message += f"Date: {update_notification.last_update}\n"
        message += f"Preview: {update_notification.preview[:200]}...\n"

        await app.tg_client.send_message(update_notification.user_id, message)
        logger.info(f"Sent notification to user {update_notification.user_id} for URL {update_notification.url}")
        return {"status": "ok"}
    except Exception as e:
        logger.exception(f"Error sending message to user {update_notification.user_id}: {e}")
        return HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)