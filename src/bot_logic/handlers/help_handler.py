from telethon import TelegramClient
from telethon.events import NewMessage
from src.bot_logic.handlers.base_handler import CommandHandler

class HelpCommandHandler(CommandHandler):
    def __init__(self, client: TelegramClient):
        super().__init__(client)

    async def execute(self, event: NewMessage.Event):
        await self.client.send_message(
            event.chat_id,
            """
            Доступные команды:
            /start - Начать работу с ботом
            /help - Показать список команд
            /track - Добавить подписку на отслеживание
            /untrack - Удалить подписку
            /list - Показать список подписок
            """,
        )

    def pattern(self):
        return "/help"