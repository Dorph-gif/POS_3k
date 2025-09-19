import psycopg2
from typing import List
from .db_service import DatabaseService
import logging
from dotenv import load_dotenv
import os

load_dotenv()

LOG_FILE = os.path.join("logs", "notification.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

class SqlDatabaseService(DatabaseService):
    def __init__(self):
        self.conn = self._get_db_connection()

    def _get_db_connection(self):
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            return conn
        except psycopg2.Error as e:
            logger.exception(f"Failed to connect to db: {e}")
            raise

    def get_users_by_link_id(self, link_id: int) -> List[int]:
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT u.telegram_id
                FROM users u
                JOIN subscriptions s ON u.user_id = s.user_id
                JOIN links l ON s.link_id = l.link_id
                WHERE s.link_id = %s
                ASC LIMIT %s OFFSET %s;
                """,
                (link_id, offset, limit,)
            )
            users = [row[0] for row in cur.fetchall()]
            return users
        except Exception as e:
            logger.exception(f"Failed to get users list for link {link_id}: {e}")
            raise