from abc import ABC, abstractmethod
from typing import List

class DatabaseService(ABC):

    @abstractmethod
    def get_users_by_link_id(self, link_id: int, offset: int, link: int) -> List[int]:
        pass