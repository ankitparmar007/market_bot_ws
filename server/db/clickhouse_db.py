import asyncio
from typing import Optional

import clickhouse_connect
from clickhouse_connect.driver import AsyncClient

from server.api.exceptions import DatabaseException
from server.api.models import SuccessResponse
from server.utils.logger import log


class ClickHouseDB:

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        retries: int = 5,
        backoff: float = 1.0,
    ) -> None:

        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database

        self._retries = retries
        self._backoff = backoff

        self._client: Optional[AsyncClient] = None

    # ---------------------------------------------------------
    # CLIENT PROPERTY
    # ---------------------------------------------------------

    @property
    def client(self) -> AsyncClient:

        if self._client is None:
            log.error("[ClickHouse.client] ❌ connection not established")
            raise DatabaseException()

        return self._client

    # ---------------------------------------------------------
    # CONNECT
    # ---------------------------------------------------------

    async def _connect(self) -> None:

        attempt = 0

        while attempt < self._retries:

            try:

                self._client = await clickhouse_connect.get_async_client(
                    host=self._host,
                    port=self._port,
                    username=self._username,
                    password=self._password,
                    database=self._database,
                )

                # verify connection
                result = await self._client.ping()

                if result:
                    log.info(f"✅ ClickHouse connected → `{self._database}`")
                    return
                else:
                    raise Exception("Pind failed")
            except Exception as e:

                attempt += 1
                wait_time = self._backoff * (2 ** (attempt - 1))

                log.warning(
                    f"⚠️ ClickHouse attempt {attempt}/{self._retries} failed: {e}. "
                    f"Retrying in {wait_time:.1f}s..."
                )

                self._client = None

                await asyncio.sleep(wait_time)

        log.error("[ClickHouse._connect] ❌ Could not connect after multiple attempts")

        raise DatabaseException()

    # ---------------------------------------------------------
    # ENSURE CONNECTION
    # ---------------------------------------------------------

    async def ensure_connection(self) -> dict:

        if self._client is None:

            await self._connect()

            return SuccessResponse(message="✅ ClickHouse connected").model_dump()

        return SuccessResponse(message="✅ ClickHouse connection is alive").model_dump()

    # ---------------------------------------------------------
    # CLOSE
    # ---------------------------------------------------------

    async def close(self) -> None:

        if self._client:

            await self._client.close()

            log.info("❌ ClickHouse connection closed")

            self._client = None
