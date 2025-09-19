from telethon import TelegramClient
from telethon.events import NewMessage
import os
from dotenv import load_dotenv
import logging
import requests
import asyncio
import redis.asyncio as aioredis

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

class ListCommandHandler(CommandHandler):
    def __init__(self, client: TelegramClient):
        super().__init__(client)
        self.redis = aioredis.from_url("redis://redis:6379", decode_responses=True)

    async def execute(self, event: NewMessage.Event):
        chat_id = event.chat_id
        redis_key = f"user_subscriptions:{chat_id}"
        server_client = get_server_client()

        try:
            cached = await self.redis.get(redis_key)
            if cached:
                await self.client.send_message(chat_id, f"Вот список ваших подписок (из кэша):\n{cached}")
                logger.info(f"Returned cached subscriptions for user_id={chat_id}")
                return

            try:
                subscriptions = await asyncio.to_thread(server_client.get_subscriptions, chat_id)
            except requests.exceptions.HTTPError as http_err:
                if http_err.response.status_code == 404:
                    await self.client.send_message(chat_id, "У вас нет подписок.")
                    return
                else:
                    logger.error(f"HTTP error: {http_err}")
                    await self.client.send_message(chat_id, "Ошибка при получении данных с сервера.")
                    return
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                await self.client.send_message(chat_id, "Сервер временно недоступен. Попробуйте позже.")
                return

            if not subscriptions:
                await self.client.send_message(chat_id, "У вас нет подписок.")
                return

            message = "\n".join(f"{i+1}. {s['url']}" for i, s in enumerate(subscriptions))
            await self.client.send_message(chat_id, f"Вот список ваших подписок:\n{message}")

            await self.redis.set(redis_key, message, ex=300)
            logger.info(f"Cached subscriptions for user_id={chat_id}")

        except Exception as e:
            logger.exception("Ошибка при обработке команды /list")
            await self.client.send_message(chat_id, "Произошла ошибка при обработке запроса.")

    def pattern(self):
        return "/list"