from abc import ABC, abstractmethod
from typing import List, Dict

class DatabaseService(ABC):

    @abstractmethod
    async def add_subscription(self, user_id: int, url: str, tags: list = None, filters: list = None) -> None:
        pass

    @abstractmethod
    async def delete_subscription(self, user_id: int, url: str) -> None:
        pass

    @abstractmethod
    async def get_subscriptions(self, user_id: int) -> List[Dict]:
        pass

    @abstractmethod
    async def get_links(self, offset: int, limit: int) -> List[Dict]:
        pass

    @abstractmethod
    async def update_last_checked_at(self, link_id: int) -> None:
        pass

    @abstractmethod
    async def get_all_user_ids(self) -> List[int]:
        pass
