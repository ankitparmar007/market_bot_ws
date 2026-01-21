import aiohttp
from typing import Any, Optional, Dict

from server.api.exceptions import AppException


class APIClient:
    def __init__(self) -> None:
        self._timeout = aiohttp.ClientTimeout(total=60)
        self._session: aiohttp.ClientSession | None = None

    async def start(self):
        if self._session is None or self._session.closed:
            print(f"✅ Starting new APIClient session...")
            self._session = aiohttp.ClientSession(timeout=self._timeout)

    async def close(self):
        if self._session and not self._session.closed:
            print(f"🔌 Closing APIClient session...")
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if not self._session:
            raise RuntimeError("APIClient not started")
        return self._session

    async def get_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Perform an async GET request and return raw bytes."""
        try:
            async with self.session.get(
                url,
                headers=headers,
                params=params,
                timeout=self._timeout,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientResponseError as e:
            raise AppException(
                f"[APIClient.get_json] HTTP Error {e.status} for URL {url}: {e.message}"
            ) 
        except aiohttp.ClientError as e:
            raise AppException(f"[APIClient.get_json] Error fetching data from {url}: {e}")

    async def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """Perform an async GET request and return raw bytes."""
        try:
            async with self.session.get(
                url,
                headers=headers,
                params=params,
                timeout=self._timeout,
            ) as resp:
                return await resp.read()
        except aiohttp.ClientResponseError as e:
            raise AppException(
                f"HTTP Error {e.status} for URL {url}: {e.message}"
            ) from e
        except aiohttp.ClientError as e:
            raise AppException(f"Error fetching data from {url}: {e}")

    async def post(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Perform an async POST request and return JSON."""
        async with self.session.post(
            url,
            headers=headers,
            data=data,
            timeout=self._timeout,
        ) as resp:
            return await resp.json()
