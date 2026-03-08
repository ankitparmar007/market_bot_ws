from typing import Any, Sequence
from server.api.exceptions import DatabaseException
from server.utils.logger import log
from server.db import clickhouse_client


class _Tables:

    def __init__(self, name: str) -> None:
        self.table_name = name

    # ---------------------------------------------------------
    # QUERY
    # ---------------------------------------------------------

    async def query(
        self, sql: str, parameters: dict | None = None
    ) -> Sequence[Sequence[Any]]:

        try:

            result = await clickhouse_client.client.query(
                sql,
                parameters=parameters,
            )

            return result.result_rows

        except Exception as e:

            log.exception(f"[ClickHouse.query] {self.table_name} failed: {e}")

            raise DatabaseException()

    # ---------------------------------------------------------
    # INSERT
    # ---------------------------------------------------------

    async def insert(self, rows: list, column_names: list):

        try:

            await clickhouse_client.client.insert(
                self.table_name,
                rows,
                column_names=column_names,
            )

        except Exception as e:

            log.exception(f"[ClickHouse.insert] {self.table_name} failed: {e}")

            raise DatabaseException()

    # ---------------------------------------------------------
    # INSERT ONE
    # ---------------------------------------------------------

    async def insert_one(self, row: list, columns: list):

        try:

            await clickhouse_client.client.insert(
                self.table_name,
                [row],
                column_names=columns,
            )

        except Exception as e:

            log.exception(f"[ClickHouse.insert_one] {self.table_name} failed: {e}")

            raise DatabaseException()

    # ---------------------------------------------------------
    # RAW SQL
    # ---------------------------------------------------------

    async def execute(self, sql: str):

        try:

            await clickhouse_client.client.command(sql)

        except Exception as e:

            log.exception(f"[ClickHouse.execute] {self.table_name} failed: {e}")

            raise DatabaseException()


class Tables:
    volume_history = _Tables("volume_history")
    intraday_history = _Tables("intraday_history")
