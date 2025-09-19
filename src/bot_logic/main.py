import asyncio
import logging

from telethon import TelegramClient, events
from fastapi import Request
import uvicorn
from dotenv import load_dotenv
import os
import requests

from src.bot_logic.handlers import chat_id_cmd_handler
from src.settings import TGBotSettings

from src.bot_logic.handlers.base_handler import CommandHandler
from src.bot_logic.handlers.start_handler import StartCommandHandler
from src.bot_logic.handlers.help_handler import HelpCommandHandler
from src.bot_logic.handlers.track_handler import TrackCommandHandler
from src.bot_logic.handlers.untrack_handler import UntrackCommandHandler
from src.bot_logic.handlers.list_handler import ListCommandHandler

load_dotenv()

LOG_FILE = os.path.join("logs", "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

settings = TGBotSettings()

client = TelegramClient("bot_session", settings.api_id, settings.api_hash).start(
    bot_token=settings.token,
)

async def register_handlers():
    start_handler = StartCommandHandler(client)
    help_handler = HelpCommandHandler(client)
    track_handler = TrackCommandHandler(client)
    untrack_handler = UntrackCommandHandler(client)
    list_handler = ListCommandHandler(client)

    client.add_event_handler(
        chat_id_cmd_handler,
        events.NewMessage(pattern="/chat_id"),
    )
    client.add_event_handler(
        start_handler.execute,
        events.NewMessage(pattern=start_handler.pattern()),
    )
    client.add_event_handler(
        help_handler.execute,
        events.NewMessage(pattern=help_handler.pattern()),
    )
    client.add_event_handler(
        track_handler.execute,
        events.NewMessage(pattern=track_handler.pattern()),
    )
    client.add_event_handler(
        track_handler.handle_url,
        events.NewMessage(func=lambda e: track_handler.conversation_states.get(e.chat_id) == "waiting_for_url")
    )
    client.add_event_handler(
        untrack_handler.execute,
        events.NewMessage(pattern=untrack_handler.pattern()),
    )
    client.add_event_handler(
        list_handler.execute,
        events.NewMessage(pattern=list_handler.pattern()),
    )

async def main() -> None:
    await register_handlers()
    while True:
        try:
            await asyncio.sleep(60)
        except Exception as e:
            logger.exception(f"Error in main loop: {e}")
            await asyncio.sleep(60)

logger.info("Run the event loop to start receiving messages")

try:
    client.start(bot_token=settings.token)
    with client:
        try:
            client.loop.run_until_complete(main())
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            logger.exception(
                "Main loop raised error.",
                extra={"exc": exc},
            )
finally:
    client.disconnect()
    logger.info("Bot stopped")