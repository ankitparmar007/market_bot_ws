# from server.api.exceptions import NotFoundException

# from server.db.collections import Collections
# from server.utils.time_tracker import timing_decorator


# class ExpiryRepository:

#     _expiry: str | None = None   # <--- in-memory cache

#     @staticmethod
#     @timing_decorator
#     async def get_expiry(refresh: bool = False) -> str:
#         # If cached, return directly
#         if ExpiryRepository._expiry and not refresh:
#             return ExpiryRepository._expiry

#         # Else fetch from DB
#         res = await Collections.expiry.find_one(
#             {"month": 'current'}, {"_id": 0, "expiry": 1}
#         )   
#         if res:
#             # Store in cache
#             expiry = res.get("expiry", "")
#             ExpiryRepository._expiry = expiry
#             return expiry
            
#         else:
#             raise NotFoundException("[get_expiry] No expiry found")

    