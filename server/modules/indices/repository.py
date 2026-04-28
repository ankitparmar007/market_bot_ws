from server.api.exceptions import NotFoundException
from server.db.collections import Collections


from server.modules.indices.models import IndicesModel
from server.utils.time_tracker import timing_decorator


class IndicesRepository:

    @staticmethod
    @timing_decorator
    async def all_indices(only_option=False) -> list[IndicesModel]:
        indices: list[IndicesModel] = []

        if only_option:
            docs = await Collections.indices.find({"has_option": True}, {"_id": 0})

            for doc in docs:
                indices.append(IndicesModel(**doc))
        else:

            docs = await Collections.indices.find({}, {"_id": 0})

            for doc in docs:
                indices.append(IndicesModel(**doc))

        if indices:
            return indices
        else:
            raise NotFoundException("No indices found")
