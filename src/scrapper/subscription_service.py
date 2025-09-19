import logging
import asyncio
from src.scrapper.stackoverflow_client import StackOverflowClient
from src.scrapper.github_client import GitHubClient
from src.scrapper.utils import is_valid_url, is_already_tracked, generate_subscription_id, extract_stackoverflow_question_id, extract_github_owner_and_repo
from src.scrapper.database.db_service import DatabaseService
from abc import ABC, abstractmethod
import datetime
import os
import aiohttp
import pytz

LOG_FILE = os.path.join("logs", "scrapper.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

class SubscriptionManager:
    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service

    async def add_subscription(self, user_id: int, url: str, tags: list = None, filters: list = None):
        if not is_valid_url(url):
            raise ValueError("Invalid URL: URL must be a StackOverflow question or a GitHub repository.")

        subscriptions = await self.get_subscriptions(user_id)
        if any(sub["url"] == url for sub in subscriptions):
            raise ValueError("URL is already being tracked for this user.")

        subscription_id = generate_subscription_id()
        subscription = {
            "id": subscription_id,
            "url": url,
            "tags": tags,
            "filters": filters,
            "last_updated_time": datetime.datetime.utcnow().isoformat(),
            "user_id": user_id,
        }

        try:
            await self.db_service.add_subscription(user_id, url, tags, filters)
            logger.info(f"Added subscription {subscription_id} for user {user_id} to URL: {url}")
            return subscription
        except Exception as e:
            logger.exception(f"Failed to add subscription to the database: {e}")
            raise

    async def delete_subscription(self, user_id: int, url: str):
        try:
            await self.db_service.delete_subscription(user_id, url)
            logger.info(f"Deleted subscription for user {user_id} to URL: {url}")
        except Exception as e:
            logger.exception(f"Failed to delete subscription from the database: {e}")
            raise

    async def get_subscriptions(self, user_id: int):
        try:
            subscriptions = await self.db_service.get_subscriptions(user_id)
            return subscriptions
        except Exception as e:
            logger.exception(f"Failed to get subscriptions from the database: {e}")
            raise

    async def get_links(self, offset: int, limit: int):
        try:
            return await self.db_service.get_links(offset, limit)
        except Exception as e:
            logger.exception(f"Failed to get links from the database: {e}")
            raise

    async def update_last_checked_at(self, link_id: int):
        try:
            await self.db_service.update_last_checked_at(link_id)
        except Exception as e:
            logger.exception(f"Failed to get links from the database: {e}")
            raise

class AbstractUpdateChecker(ABC):
    @abstractmethod
    async def check_for_updates(self, link: dict) -> bool:
        pass

class StackOverflowUpdateChecker(AbstractUpdateChecker):
    def __init__(self, stackoverflow_client: StackOverflowClient):
        self.stackoverflow_client = stackoverflow_client

    async def check_for_updates(self, link: dict) -> bool:
        url = link["url"]
        question_id = extract_stackoverflow_question_id(url)
        logger.info(f"Checking updates for StackOverflow question {question_id}")
        last_activity_date = await self.stackoverflow_client.get_last_activity_date(question_id)

        if last_activity_date and last_activity_date > datetime.datetime.fromisoformat(link["last_checked_at"].replace("Z", "+00:00")):
            logger.info(f"Update found for StackOverflow question {question_id}")
            return True
        else:
            return False

class GitHubUpdateChecker(AbstractUpdateChecker):
    def __init__(self, github_client: GitHubClient):
        self.github_client = github_client

    async def check_for_updates(self, link: dict) -> bool:
        url = link["url"]
        owner, repo = extract_github_owner_and_repo(url)
        logger.info(f"Checking updates for github repo{repo}, owner {owner}")
        last_push_date = await self.github_client.get_last_change_date(owner, repo)

        last_checked_at = link.get("last_checked_at")

        if last_push_date and last_push_date.tzinfo:
            last_checked_at = last_checked_at.replace(tzinfo=last_push_date.tzinfo)
        else:
            last_checked_at = last_checked_at.replace(tzinfo=pytz.UTC)

        if last_push_date and last_push_date > last_checked_at:
            logger.info(f"Update found for GitHub repository {owner}/{repo}")
            return True
        else:
            return False

class SubscriptionService:
    def __init__(self, db_service: DatabaseService, stackoverflow_client: StackOverflowClient, github_client: GitHubClient):
        self.subscription_manager = SubscriptionManager(db_service)
        self.db_service = db_service
        self.stackoverflow_client = stackoverflow_client
        self.github_client = github_client
        self.stackoverflow_update_checker = StackOverflowUpdateChecker(self.stackoverflow_client)
        self.github_update_checker = GitHubUpdateChecker(self.github_client)

    async def add_subscription(self, user_id: int, url: str, tags: list = None, filters: list = None):
        return await self.subscription_manager.add_subscription(user_id, url, tags, filters)

    async def delete_subscription(self, user_id: int, url: str):
        await self.subscription_manager.delete_subscription(user_id, url)

    async def get_subscriptions(self, user_id: int):
        return await self.subscription_manager.get_subscriptions(user_id)

    async def check_updates(self):
        logger.info("Checking for updates...")

        offset = 0
        limit = int(os.getenv("BATCH_SIZE", 500))
        while True:
            try:
                links = await self.subscription_manager.get_links(offset, limit)

                offset += limit

                if len(links) == 0:
                    break

                for link in links:
                    has_updates = await self._check_for_updates(link)

                if has_updates:
                    try:
                        await self._send_update_notification(link)
                    except Exception as e:
                        break

                    await self.subscription_manager.update_last_checked_at(link["link_id"])
            except Exception as e:
                logger.exception(f"Failed to check updates: {e}")
                break

    async def _check_for_updates(self, link: dict) -> bool:
        url = link["url"]
        if "stackoverflow.com" in url:
            return await self.stackoverflow_update_checker.check_for_updates(link)
        elif "github.com" in url:
            return await self.github_update_checker.check_for_updates(link)
        else:
            logger.warning(f"Unsupported URL: {url}")
            return False

    async def _send_update_notification(self, link: dict):
        link_id = link["link_id"]
        url = link["url"]
        last_updated_time = link["last_checked_at"]

        title = None
        user_name = None
        preview = None
        notification_type = None

        try:
            if "stackoverflow.com" in url:
                question_id = extract_stackoverflow_question_id(url)
                question_data = await self.stackoverflow_client.extract_question_data(question_id)
                if question_data:
                    title = question_data["title"]
                    user_name = question_data["user_name"]
                    preview = question_data["preview"]
                    notification_type = "stackoverflow"
            elif "github.com" in url:
                owner, repo = extract_github_owner_and_repo(url)
                repo_data = await self.github_client.get_last_update_info(owner, repo)
                if repo_data:
                    title = repo_data["title"]
                    user_name = repo_data["user_name"]
                    preview = repo_data["preview"]
                    notification_type = "github"

            last_updated_time_str = last_updated_time.isoformat() if isinstance(last_updated_time, datetime.datetime) else str(last_updated_time)

            update_notification = {
                "link_id": link_id,
                "url": url,
                "last_update": last_updated_time_str,
                "title": title,
                "user_name": user_name,
                "preview": preview,
            }

            logger.info(f"Sent update notification to notification_service {update_notification}")

            await self._send_notification_to_service(update_notification)
        except Exception as e:
            logger.exception(f"Failed to send update notification: {e}")
            raise e
        
    async def _send_notification_to_service(self, update_notification: dict):
        NOTIFICATION_URL = os.getenv("NOTIFICATION_URL")
        logger.info(f"Sent update notification to notification_service \n url: {NOTIFICATION_URL}/api/v1/link_updated, notification: {update_notification}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{NOTIFICATION_URL}/api/v1/link_updated", json=update_notification) as response:
                    response.raise_for_status()
                    logger.info(f"Successfully sent update notification to notification_service")
        except Exception as e:
            logger.exception(f"Failed to send update notification to notification_service: {e}")
            raise e
