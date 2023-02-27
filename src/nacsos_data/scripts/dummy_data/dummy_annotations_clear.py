from nacsos_data.db.connection import get_engine
from nacsos_data.db.schemas import AssignmentScope
from sqlalchemy import select

SCOPE_ID = 'ab943b33-b37a-4939-a7ef-6e521d37de12'
engine = get_engine(conf_file='../../nacsos-core/config/default.env')
session = engine()

scope = session.scalars(select(AssignmentScope).where(AssignmentScope.assignment_scope_id == SCOPE_ID)).first()
session.delete(scope)
session.commit()
