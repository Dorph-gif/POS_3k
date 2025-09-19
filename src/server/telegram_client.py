import logging
import os

from telethon import TelegramClient
from telethon.errors.rpcerrorlist import ApiIdInvalidError

LOG_FILE = os.path.join("logs", "server.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

class TelegramClientManager:
    def __init__(self, session_name: str, api_id: int, api_hash: str, bot_token: str):
        self.session_name = session_name
        self.api_id = api_id
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.client = None

    async def __aenter__(self):
        self.client = TelegramClient(
            self.session_name,
            self.api_id,
            self.api_hash,
        )
        try:
            await self.client.start(bot_token=self.bot_token)
            logger.info("Telegram client started")
            return self.client
        except ApiIdInvalidError:
            logger.error("Api ID is invalid. Check your settings.")
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.disconnect()
            logger.info("Telegram client disconnected")

    async def send_message(self, user_id: int, message: str):
        try:
            await self.client.send_message(user_id, message)
            logger.info(f"Sent message to user {user_id}")
        except Exception as e:
            logger.exception(f"Failed to send message to user {user_id}: {e}")
            raise