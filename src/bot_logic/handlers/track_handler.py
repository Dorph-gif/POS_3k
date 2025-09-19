import asyncio
import logging
import os
import json
import redis.asyncio as redis

import requests
from telethon import TelegramClient, events
from telethon.events import NewMessage

from src.bot_logic.handlers.base_handler import CommandHandler
from src.bot_logic.server_client import get_server_client

from dotenv import load_dotenv

load_dotenv()

LOG_FILE = os.path.join("logs", "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

class TrackCommandHandler(CommandHandler):
    def __init__(self, client: TelegramClient):
        super().__init__(client)
        self.conversation_states = {}
        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True
        )

    async def execute(self, event: NewMessage.Event):
        chat_id = event.chat_id
        self.conversation_states[chat_id] = "waiting_for_url"
        await self.client.send_message(chat_id, "Пожалуйста, введите URL для отслеживания:")
        raise events.StopPropagation

    def pattern(self):
        return "/track"

    async def handle_url(self, event: NewMessage.Event):
        chat_id = event.chat_id
        url = event.message.text

        if not url:
            await self.client.send_message(chat_id, "Вы должны ввести URL.")
            self.conversation_states.pop(chat_id, None)
            return

        try:
            server_client = get_server_client()
            result = await asyncio.to_thread(server_client.create_subscription, url, chat_id)
            await self.client.send_message(chat_id, "URL успешно добавлен для отслеживания!")
            logger.info(f"Subscription created: {result}")

            await self.redis.delete(f"user_subscriptions:{chat_id}")

        except Exception as e:
            logger.exception("Failed to send URL to server.")
            await self.client.send_message(chat_id, f"Произошла ошибка при отправке URL.")
        finally:
            self.conversation_states.pop(chat_id, None)