import aiohttp
import asyncio
from typing import Any, Optional, Dict

from server.api.exceptions import BadRequestException


class APIClient:
    def __init__(self, timeout: int = 60) -> None:
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(timeout=self._timeout)
                print(f"✅ APIClient started")

    async def close(self) -> None:
        async with self._lock:
            if self._session and not self._session.closed:
                await self._session.close()
                print(f"❌ APIClient closed")
            self._session = None

    async def __aenter__(self) -> "APIClient":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            raise RuntimeError("APIClient not started")
        return self._session

    async def get_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            async with self.session.get(
                url,
                headers=headers,
                params=params,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientResponseError as e:
            raise BadRequestException(f"HTTP {e.status} error for {url}") from e
        except aiohttp.ClientError as e:
            raise BadRequestException(f"Network error for {url}: {e}") from e

    async def get_bytes(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        try:
            async with self.session.get(
                url,
                headers=headers,
                params=params,
            ) as resp:
                resp.raise_for_status()
                return await resp.read()
        except aiohttp.ClientResponseError as e:
            raise BadRequestException(f"HTTP {e.status} error for {url}") from e
        except aiohttp.ClientError as e:
            raise BadRequestException(f"Network error for {url}: {e}") from e

    async def post_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            async with self.session.post(
                url,
                headers=headers,
                json=json,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientResponseError as e:
            raise BadRequestException(f"HTTP {e.status} error for {url}") from e
        except aiohttp.ClientError as e:
            raise BadRequestException(f"Network error for {url}: {e}") from e
