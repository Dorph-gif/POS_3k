from telethon import TelegramClient
from telethon.events import NewMessage
import os
from dotenv import load_dotenv
import logging
import requests
import asyncio
import redis.asyncio as redis

from src.bot_logic.handlers.base_handler import CommandHandler
from src.bot_logic.server_client import get_server_client

load_dotenv()

LOG_FILE = os.path.join("logs", "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

class UntrackCommandHandler(CommandHandler):
    def __init__(self, client: TelegramClient):
        super().__init__(client)
        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True
        )

    async def execute(self, event: NewMessage.Event):
        chat_id = event.chat_id
        parts = event.message.text.split(' ')
        if len(parts) < 2:
            await self.client.send_message(chat_id, "Вы должны ввести URL после команды /untrack.")
            return

        url = parts[1]
        logger.info(f"Get untrack url: {url}")

        try:
            server_client = get_server_client()
            await asyncio.to_thread(server_client.delete_subscription, url, chat_id)
            await self.client.send_message(chat_id, "URL успешно удален!")
            logger.info(f"Subscription deleted: user_id={chat_id}, url={url}")
            await self.redis.delete(f"user_subscriptions:{chat_id}")

        except Exception as e:
            logger.exception("Failed to delete subscription.")
            await self.client.send_message(chat_id, f"Произошла ошибка при удалении URL.")

    def pattern(self):
        return "/untrack"