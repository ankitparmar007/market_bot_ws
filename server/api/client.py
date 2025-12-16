import aiohttp
from typing import Any, Optional, Dict

class APIClient:
    """Strongly typed async HTTP client for aiohttp."""

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        # Define a default timeout (can be reused for all requests)
        self._timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=60)

    async def __aenter__(self) -> "APIClient":
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("Session not initialized. Use 'async with APIClient()'.")
        return self._session

    async def get_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> dict:
        """Perform an async GET request and return raw bytes."""
        async with self.session.get(
            url,
            headers=headers,
            params=params,
            timeout=self._timeout,
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> bytes:
        """Perform an async GET request and return raw bytes."""
        async with self.session.get(
            url,
            headers=headers,
            params=params,
            timeout=self._timeout,
        ) as resp:
            resp.raise_for_status()
            return await resp.read()

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
            resp.raise_for_status()
            return await resp.json()
