import abc
from telethon import TelegramClient

class CommandHandler(abc.ABC):
    def __init__(self, client: TelegramClient):
        self.client = client

    @abc.abstractmethod
    async def execute(self, event):
        """
        Abstract method to handle command execution.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def pattern(self):
        """
        Abstract method to define command pattern.
        """
        raise NotImplementedError