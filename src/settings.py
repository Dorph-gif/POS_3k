import typing
from pathlib import Path
import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()
__all__ = ("TGBotSettings",)


class TGBotSettings(BaseSettings):
    debug: bool = Field(default=False)

    api_id: int = Field(os.getenv("BOT_API_ID"))
    api_hash: str = Field(os.getenv("BOT_API_HASH"))
    token: str = Field(os.getenv("BOT_TOKEN"))

    model_config: typing.ClassVar[SettingsConfigDict] = SettingsConfigDict(
        extra="ignore",
        frozen=True,
        case_sensitive=False,
        env_file=Path(__file__).parent.parent / ".env",
        env_prefix="BOT_",
    )
