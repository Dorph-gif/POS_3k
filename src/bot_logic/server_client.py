import requests
from requests.exceptions import RequestException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import os
import pybreaker
from dotenv import load_dotenv

load_dotenv()

LOG_FILE = os.path.join("logs", "server.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
)
logger = logging.getLogger(__name__)

def get_server_client():
    return ServerClient(os.getenv("SERVER_URL", "http://server:8000"))

class BaseHTTPClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 3.0,
        retries: int = 3,
        backoff_factor: float = 0.5
    ):
        self.base_url = base_url
        self.timeout = timeout

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.breaker = pybreaker.CircuitBreaker(
            fail_max=1,
            reset_timeout=1,
            exclude=["ValueError"]
        )

    def _build_url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _handle_response(self, response: requests.Response) -> dict:
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error: {err}")
            raise
        except requests.exceptions.JSONDecodeError as err:
            logger.error(f"Json decode error: {err}")
            raise

    def _fallback(self, method: str, path: str, **kwargs) -> dict:
        logger.warning(f"Fallback triggered for {method} {path}. Returning default response.")
        return {"status": "fallback", "message": f"{method} {path} failed"}

    def get(self, path: str, params: dict = None) -> dict:
        url = self._build_url(path)

        try:
            return self.breaker.call(
                lambda: self._handle_response(
                    self.session.get(url, params=params, timeout=self.timeout)
                )
            )
        except (RequestException, pybreaker.CircuitBreakerError) as err:
            logger.error(f"GET request failed: {err}")
            return self._fallback("GET", path)

    def post(self, path: str, json: dict = None) -> dict:
        url = self._build_url(path)

        try:
            return self.breaker.call(
                lambda: self._handle_response(
                    self.session.post(url, json=json, timeout=self.timeout)
                )
            )
        except (RequestException, pybreaker.CircuitBreakerError) as err:
            logger.error(f"POST request failed: {err}")
            return self._fallback("POST", path)

    def put(self, path: str, json: dict = None) -> dict:
        url = self._build_url(path)

        try:
            return self.breaker.call(
                lambda: self._handle_response(
                    self.session.put(url, json=json, timeout=self.timeout)
                )
            )
        except (RequestException, pybreaker.CircuitBreakerError) as err:
            logger.error(f"PUT request failed: {err}")
            return self._fallback("PUT", path)

    def delete(self, path: str, params: dict = None) -> dict:
        url = self._build_url(path)

        try:
            return self.breaker.call(
                lambda: self._handle_response(
                    self.session.delete(url, params=params, timeout=self.timeout)
                )
            )
        except (RequestException, pybreaker.CircuitBreakerError) as err:
            logger.error(f"DELETE request failed: {err}")
            return self._fallback("DELETE", path)

class ServerClient(BaseHTTPClient):
    def __init__(self, base_url: str):
        super().__init__(base_url)

    def get_subscriptions(self, user_id: int):
        path = f"/api/v1/subscriptions/{user_id}"
        return self.get(path)
    
    def create_subscription(self, url: str, user_id: int):
        path = f"/api/v1/subscriptions/"
        payload = {"url": url, "user_id": user_id}
        return self.post(path, json=payload)
    
    def delete_subscription(self, url: str, user_id: int):
        path = f"/api/v1/subscriptions/{user_id}?url={url}"
        return self.delete(path)