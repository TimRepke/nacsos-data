import uuid

from nacsos_data.models.priority import PriorityModel
from sqlalchemy import select

from nacsos_data.db.engine import ensure_session_async, DBSession
from nacsos_data.db.schemas import Priority


@ensure_session_async
async def read_priority_by_id(session: DBSession, priority_id: str | uuid.UUID) -> PriorityModel | None:
    stmt = select(Priority).where(Priority.priority_id == priority_id)
    result = (await session.execute(stmt)).scalars().one_or_none()
    if result is not None:
        return PriorityModel.model_validate(result.__dict__)
    return None
