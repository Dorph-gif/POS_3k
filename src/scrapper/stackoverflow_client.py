import aiohttp
import asyncio
import logging
import os
import datetime
import json

LOG_FILE = os.path.join("logs", "scrapper.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

class StackOverflowClient:
    def __init__(self, api_url: str = "https://api.stackexchange.com/2.3"):
        self.api_url = api_url
        self.api_key = os.getenv("STACKOVERFLOW_API_KEY")

    async def get_question(self, question_id: int):
        url = f"{self.api_url}/questions/{question_id}?site=stackoverflow&filter=!nKzQ3Wz8RH"
        if self.api_key:
            url += f"&key={self.api_key}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if data['items']:
                        return data['items'][0]
                    else:
                        logger.warning(f"No data found for question {question_id}")
                        return None
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching StackOverflow question {question_id}: {e}")
            raise

    async def get_last_activity_date(self, question_id: int) -> datetime.datetime | None:
        question_data = await self.get_question(question_id)
        if not question_data:
            return None

        try:
            last_activity_date = datetime.datetime.fromtimestamp(question_data['last_activity_date'])
            return last_activity_date
        except KeyError:
            logger.warning(f"No 'last_activity_date' found for question {question_id}")
            return None

    async def extract_question_data(self, question_id: int):
        question_data = await self.get_question(question_id)
        if not question_data:
            return None

        try:
            title = question_data['title']
            owner_name = question_data['owner']['display_name']
            body = question_data['body']

            return {
                "title": title,
                "user_name": owner_name,
                "preview": body[:200]
            }
        except KeyError as e:
            logger.error(f"Missing key in question {e}")
            return None