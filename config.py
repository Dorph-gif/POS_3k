import os
import dotenv

load_dotenv()

BOT_API_ID=os.getenv("BOT_API_ID", "22244987")
BOT_API_HASH=os.getenv("BOT_API_HASH", "5b2b7b9575535af6cbdb3335a5d95366")
BOT_TOKEN=os.getenv("BOT_TOKEN", "7698584724:AAFWGnZlNt88TsZvxXrd0fcm7o8VfU5ntTM")
LOGGING_LEVEL=os.getenv("LOGGING_LEVEL", "INFO")
SCRAPPER_URL=os.getenv("SCRAPPER_URL", "http://localhost:8001")
SERVER_URL=os.getenv("SERVER_URL", "http://localhost:8000")
NOTIFICATION_URL=os.getenv("NOTIFICATION_URL", "http://localhost:8002")
GITHUB_TOKEN=os.getenv("GITHUB_TOKEN")
STACKOVERFLOW_API_KEY=os.getenv("STACKOVERFLOW_API_KEY")
DB_HOST=os.getenv("DB_HOST", "localhost")
DB_PORT=os.getenv("DB_PORT", "5433")
DB_NAME=os.getenv("DB_NAME", "mydb")
DB_USER=os.getenv("DB_USER", "user")
DB_PASSWORD=os.getenv("DB_PASSWORD", "password")
ACCESS_TYPE=os.getenv("ACCESS_TYPE", "SQL")