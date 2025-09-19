import aiohttp
import asyncio
import logging
import os
import datetime

LOG_FILE = os.path.join("logs", "scrapper.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

class GitHubClient:
    def __init__(self, api_url: str = "https://api.github.com"):
        self.api_url = api_url
        self.token = os.getenv("GITHUB_TOKEN")

    async def get_latest_changes(self, repo_owner: str, repo_name: str, count: int = 5):
        url = f"{self.api_url}/repos/{repo_owner}/{repo_name}/issues"
        headers = {}
        if self.token:
            headers["Authorization"] = f"token {self.token}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params={'state': 'all', 'sort': 'updated', 'direction': 'desc', 'per_page': count}) as response:
                    response.raise_for_status()
                    changes = await response.json()
                    return changes
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching issues and pull requests for {repo_owner}/{repo_name}: {e}")
            return []

    async def extract_change_data(self, change: dict, repo_owner: str, repo_name: str):
        title = change.get('title')
        user_name = change.get('user', {}).get('login')
        issue_number = change.get('number')
        preview = change.get('body') or ""

        comments_url = f"{self.api_url}/repos/{repo_owner}/{repo_name}/issues/{issue_number}/comments"
        headers = {}
        if self.token:
            headers["Authorization"] = f"token {self.token}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(comments_url, headers=headers) as resp:
                    if resp.status == 200:
                        comments = await resp.json()
                        if comments:
                            last_comment = comments[-1]
                            preview = last_comment.get("body", preview)
                            user_name = last_comment.get("user", {}).get("login", user_name)
        except aiohttp.ClientError as e:
            logger.warning(f"Failed to fetch comments for issue #{issue_number}: {e}")

        change_type = "pull_request" if "pull_request" in change else "issue"

        return {
            "title": title,
            "user_name": user_name,
            "preview": preview,
        }

    async def get_last_change_date(self, repo_owner: str, repo_name: str):
        changes = await self.get_latest_changes(repo_owner, repo_name, count=1)
        if changes:
            last_change = changes[0]
            date_str = last_change.get('updated_at')
            if date_str:
                change_date = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return change_date
            else:
                logger.warning(f"No 'updated_at' field found for latest change in {repo_owner}/{repo_name}")
                return None
        else:
            logger.warning(f"No issues or pull requests found for {repo_owner}/{repo_name}")
            return None

    async def get_last_update_info(self, repo_owner: str, repo_name: str) -> dict | None:
        changes = await self.get_latest_changes(repo_owner, repo_name, count=1)
        if changes:
            last_change = changes[0]
            change_data = await self.extract_change_data(last_change, repo_owner, repo_name)
            return change_data
        else:
            logger.warning(f"No issues or pull requests found for {repo_owner}/{repo_name}")
            return None

"""
class GitHubClient:
    def __init__(self, api_url: str = "https://api.github.com"):
        self.api_url = api_url
        self.token = os.getenv("GITHUB_TOKEN")

    async def get_repo(self, repo_owner: str, repo_name: str):
        url = f"{self.api_url}/repos/{repo_owner}/{repo_name}"
        headers = {}
        if self.token:
            headers["Authorization"] = f"token {self.token}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return data
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching GitHub repository {repo_owner}/{repo_name}: {e}")
            raise

    async def get_last_push_date(self, repo_owner: str, repo_name: str) -> datetime.datetime | None:
        try:
            repo_data = await self.get_repo(repo_owner, repo_name)
            logger.info(f"get repo data: {repo_data}")
            if repo_data:
                date_str = repo_data.get('pushed_at')
                if date_str:
                    return datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    logger.warning(f"No 'pushed_at' field found for {repo_owner}/{repo_name}")
                    return None
            else:
                logger.warning(f"No data returned for {repo_owner}/{repo_name}")
                return None
        except Exception as e:
            logger.exception(f"Error getting last push date for {repo_owner}/{repo_name}: {e}")
            return None

    async def extract_repo_data(self, repo_owner: str, repo_name: str):
        try:
            repo_data = await self.get_repo(repo_owner, repo_name)
            if repo_data:
                title = repo_data.get('name')
                owner_name = repo_data.get('owner', {}).get('login')
                preview = repo_data.get('description')

                if not preview:
                    preview = ""

                return {
                    "title": title,
                    "user_name": owner_name,
                    "preview": preview
                }
            else:
                logger.warning(f"No data returned for {repo_owner}/{repo_name}")
                return None
        except Exception as e:
            logger.exception(f"Error extracting repo data for {repo_owner}/{repo_name}: {e}")
            return None
"""
