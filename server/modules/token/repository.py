from server.api.exceptions import AppException

from server.db.collections import Collections
from server.modules.token.enums import Developer
from server.utils.time_tracker import timing_decorator


class TokenRepository:

    # @staticmethod
    # @timing_decorator
    # async def update_token_in_env_locally() -> dict:
    #     res = await TokenRepository._token_col.find_one({"_id": "token"}, {"_id": 0})
    #     if res:
    #         # Update or add AUTH_TOKEN_UPSTOX in .env
    #         set_key(".env", "AUTH_TOKEN_UPSTOX", res.get("access_token", ""))
    #         return SuccessResponse(message="Token updated successfully").to_dict()
    #     else:
    #         raise AppException("[get_token] Token not found")

    _cached_ankit_token: str | None = None  # <--- in-memory cache
    _cached_rachit_token: str | None = None  # <--- in-memory cache

    @staticmethod
    def get_token(dev: Developer) -> str:
        # If cached, return directly
        if TokenRepository._cached_ankit_token and dev == Developer.ANKIT:
            return TokenRepository._cached_ankit_token

        if TokenRepository._cached_rachit_token and dev == Developer.RACHIT:
            return TokenRepository._cached_rachit_token

        # Else fetch from DB
        res = Collections.token.find_one(
            {"_id": dev.value}, {"_id": 0, "updated_at": 0}
        )
        if res:
            # Store in cache
            token = res.get("access_token", "")
            if dev == Developer.ANKIT:
                TokenRepository._cached_ankit_token = token
            if dev == Developer.RACHIT:
                TokenRepository._cached_rachit_token = token
            return token
        else:
            raise AppException("[get_token] Token not found")

    @staticmethod
    @timing_decorator
    def refresh_cached_token():
        res = Collections.token.find_one(
            {"_id": Developer.ANKIT.value}, {"_id": 0, "updated_at": 0}
        )
        if res:
            # Store in cache
            token = res.get("access_token", "")
            TokenRepository._cached_ankit_token = token

        res = Collections.token.find_one(
            {"_id": Developer.RACHIT.value}, {"_id": 0, "updated_at": 0}
        )
        if res:
            # Store in cache
            token = res.get("access_token", "")
            TokenRepository._cached_rachit_token = token
