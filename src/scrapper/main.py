from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import uvicorn
import os
import logging
import asyncio
from typing import Optional, List
from prometheus_client import start_http_server, Counter
import threading

from .subscription_service import SubscriptionService
from .database.database import create_database_service, DatabaseService
from .stackoverflow_client import StackOverflowClient
from .github_client import GitHubClient
from .metrics_server import start_metrics_server, notifications_counter
from dotenv import load_dotenv

load_dotenv()

LOG_FILE = os.path.join("logs", "scrapper.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

app = FastAPI()

def get_stackoverflow_client():
    return StackOverflowClient()

def get_github_client():
    return GitHubClient()

async def get_subscription_service(
    db_service: DatabaseService = Depends(create_database_service),
    stackoverflow_client: StackOverflowClient = Depends(get_stackoverflow_client),
    github_client: GitHubClient = Depends(get_github_client)
):
    return SubscriptionService(db_service=db_service, stackoverflow_client=stackoverflow_client, github_client=github_client)

async def get_subscription_service_update(
    db_service: DatabaseService = create_database_service(),
    stackoverflow_client: StackOverflowClient =  get_stackoverflow_client(),
    github_client: GitHubClient = get_github_client()
):
    return SubscriptionService(db_service=db_service, stackoverflow_client=stackoverflow_client, github_client=github_client)


class SubscriptionRequest(BaseModel):
    url: str
    tags: Optional[List[str]] = None
    filters: Optional[List[str]] = None
    user_id: int

@app.post("/api/v1/subscriptions/")
async def create_subscription(
    subscription: SubscriptionRequest,
    subscription_service: SubscriptionService = Depends(get_subscription_service)
):
    logger.info(f"Received {subscription}")
    try:
        user_id = subscription.user_id
        url = subscription.url
        tags = subscription.tags
        filters = subscription.filters

        if not user_id:
            raise HTTPException(status_code=400, detail="User ID is required")

        subscription = await subscription_service.add_subscription(
            user_id=user_id, url=url, tags=tags, filters=filters
        )
        return {"status": "ok", "subscription": subscription}
    except Exception as e:
        logger.exception(f"Failed to create subscription: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/v1/subscriptions/{user_id}")
async def delete_subscription(
    user_id: int, url: str,
    subscription_service: SubscriptionService = Depends(get_subscription_service)
):
    try:
        await subscription_service.delete_subscription(user_id=user_id, url=url)
        return {"status": "ok"}
    except Exception as e:
        logger.exception(f"Failed to delete subscription {url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/subscriptions/{user_id}")
async def get_subscriptions(
    user_id: int,
    subscription_service: SubscriptionService = Depends(get_subscription_service)
):
    try:
        subscriptions = await subscription_service.get_subscriptions(user_id=user_id)
        return subscriptions
    except Exception as e:
        logger.exception(f"Failed to get subscriptions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/check_updates/")
async def check_updates(
    subscription_service: SubscriptionService = Depends(get_subscription_service)
):
    try:
        server_url = os.getenv("SERVER_URL")
        if not server_url:
            raise HTTPException(status_code=500, detail="SERVER_URL environment variable not set.")
        asyncio.create_task(subscription_service.check_updates(server_url))
        return {"status": "ok"}
    except Exception as e:
        logger.exception(f"Failed to check updates: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def check_updates_periodically():
    check_interval = int(os.getenv("CHECK_UPDATE_INTERVAL", 60))
    while True:
        try:
            subscription_service: SubscriptionService = await get_subscription_service_update()
            await subscription_service.check_updates()
        except Exception as e:
            logger.exception(f"Failed to check updates: {e}")
        await asyncio.sleep(int(check_interval))

@app.on_event("startup")
async def startup_event():
    subscription_service = await get_subscription_service()
    asyncio.create_task(check_updates_periodically())
    start_metrics_server()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)