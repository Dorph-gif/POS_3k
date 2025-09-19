import re
import time

def is_already_tracked(user_id: int, url: str, subscriptions: dict) -> bool:
    if user_id in subscriptions:
        for subscription in subscriptions[user_id]:
            if subscription["url"] == url:
                return True
    return False

def is_valid_url(url: str) -> bool:
    stackoverflow_regex = r"^https:\/\/stackoverflow\.com\/questions\/\d+"
    github_regex = r"^https:\/\/github\.com\/[^\/]+\/[^\/]+$"

    return re.match(stackoverflow_regex, url) is not None or re.match(github_regex, url) is not None

def generate_subscription_id():
    return int(time.time() * 1000)

def extract_stackoverflow_question_id(url: str) -> int:
    match = re.search(r"questions\/(\d+)", url)
    if match:
        return int(match.group(1))
    return None

def extract_github_owner_and_repo(url: str) -> tuple:
    match = re.search(r"github\.com\/([^\/]+)\/([^\/]+)", url)
    if match:
        return match.group(1), match.group(2)
    return None