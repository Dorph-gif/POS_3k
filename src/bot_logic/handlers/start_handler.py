from telethon import TelegramClient
from telethon.events import NewMessage
from src.bot_logic.handlers.base_handler import CommandHandler

class StartCommandHandler(CommandHandler):
    def __init__(self, client: TelegramClient):
        super().__init__(client)

    async def execute(self, event: NewMessage.Event):
        await self.client.send_message(event.chat_id, "Привет! Я бот для отслеживания изменений на сайтах.")

    def pattern(self):
        return "/start"