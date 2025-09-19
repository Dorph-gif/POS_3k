import psycopg2
from typing import List, Dict
from .db_service import DatabaseService
import logging
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

LOG_FILE = os.path.join("logs", "scrapper.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

class SqlDatabaseService(DatabaseService):
    def __init__(self):
        self.conn = self._get_db_connection()

    def _get_db_connection(self):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            return conn
        except psycopg2.Error as e:
            print(f"Error connecting to the database: {e}")
            raise

    async def add_subscription(self, user_id: int, url: str, tags: list = None, filters: list = None) -> None:
        try:
            cur = self.conn.cursor()

            cur.execute("SELECT user_id FROM users WHERE telegram_id = %s", (user_id,))
            user_result = cur.fetchone()

            if not user_result:
                cur.execute(
                    "INSERT INTO users (telegram_id) VALUES (%s) RETURNING user_id",
                    (user_id,)
                )
                telegram_id = cur.fetchone()[0]
            else:
                telegram_id = user_result[0]

            cur.execute("SELECT link_id FROM links WHERE url = %s", (url,))
            link_id_result = cur.fetchone()

            if link_id_result:
                link_id = link_id_result[0]
            else:
                if "stackoverflow.com" in url:
                    link_type = "stackoverflow"
                elif "github.com" in url:
                    link_type = "github"
                else:
                    raise ValueError("Invalid URL type")

                cur.execute(
                    "INSERT INTO links (url, type, last_checked_at) VALUES (%s, %s, NOW()) RETURNING link_id",
                    (url, link_type)
                )
                link_id = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO subscriptions (user_id, link_id, created_at) VALUES (%s, %s, NOW())",
                (telegram_id, link_id)
            )

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error adding subscription: {e}")
            raise

    async def delete_subscription(self, user_id: int, url: str) -> None:
        try:
            cur = self.conn.cursor()
            
            cur.execute("SELECT user_id FROM users WHERE telegram_id = %s", (user_id,))
            user_result = cur.fetchone()

            if not user_result:
                cur.execute(
                    "INSERT INTO users (telegram_id) VALUES (%s) RETURNING user_id",
                    (user_id,)
                )
                telegram_id = cur.fetchone()[0]
            else:
                telegram_id = user_result[0]

            cur.execute("SELECT link_id FROM links WHERE url = %s", (url,))

            link_id_result = cur.fetchone()
            link_id = link_id_result

            if not link_id:
                raise ValueError("Url not found in subscriptions")

            logger.info("Found link to delete: {link_id}")

            cur.execute(
                """
                DELETE FROM subscriptions
                WHERE user_id = %s AND link_id = %s;
                """,
                (telegram_id, link_id)
            )

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error("Error during delete: {e}")
            raise

    async def get_subscriptions(self, telegram_id: int) -> List[Dict]:
        try:
            cur = self.conn.cursor()

            cur.execute("SELECT user_id FROM users WHERE telegram_id = %s", (telegram_id,))
            user_result = cur.fetchone()

            if not user_result:
                cur.execute(
                    "INSERT INTO users (telegram_id) VALUES (%s) RETURNING user_id",
                    (telegram_id,)
                )
                user_id = cur.fetchone()[0]
            else:
                user_id = user_result[0]

            cur.execute(
                """
                SELECT l.url, l.type, s.created_at
                FROM subscriptions s
                JOIN links l ON s.link_id = l.link_id
                WHERE s.user_id = %s;
                """,
                (user_id,)
            )

            subscriptions = []
            for row in cur.fetchall():
                subscriptions.append({
                    "url": row[0],
                    "type": row[1],
                    "created_at": row[2].isoformat()
                })

            return subscriptions
        except Exception as e:
            print(f"Error getting subscriptions: {e}")
            raise

    async def get_links(self, offset: int, limit: int) -> List[Dict]:
        try:
            cur = self.conn.cursor()
            limit = int(limit)
            offset = int(offset)
            cur.execute(
                "SELECT link_id, url, last_checked_at FROM links ORDER BY last_checked_at ASC LIMIT %s OFFSET %s",
                (limit, offset),
            )

            column_names = [desc[0] for desc in cur.description]
            links = [dict(zip(column_names, row)) for row in cur.fetchall()]

            return links
        except Exception as e:
            print(f"Error getting links: {e}")
            raise

    async def update_last_checked_at(self, link_id: int) -> None:
        try:
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE links SET last_checked_at = NOW() WHERE link_id = %s",
                (link_id,),
            )
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error updating last_checked_at for link_id {link_id}: {e}")
            raise

    async def get_all_user_ids(self) -> List[int]:
        try:
            cur = self.conn.cursor()

            cur.execute("SELECT DISTINCT user_id FROM subscriptions")

            user_ids = [row[0] for row in cur.fetchall()]
            return user_ids
        except Exception as e:
            print(f"Error getting all user IDs: {e}")
            raise