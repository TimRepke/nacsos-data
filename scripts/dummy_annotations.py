from sqlalchemy import text, select, insert
from nacsos_data.db.connection import get_engine
from nacsos_data.db.schemas import Assignment, AssignmentScope, AnnotationScheme, Annotation, Project, User, \
    M2MProjectItem

PROJECT_ID = '59577b91-5d6d-4460-9074-4cf2e4bd748c'
SCHEME_ID = '98545fb3-3ed3-48b2-9748-a24015244be0'
USER_IDS = ['b0949d0e-e3e1-47c3-9a5d-a2cbbdc2ea23',
            '3c6eed89-e0c1-43b4-9d56-f12382fe65ef',
            '9e896c7c-4c38-4a6e-950c-62f09e1a6511']

engine = get_engine(conf_file='../../nacsos-core/config/default.env')


def annotate(sess, uid, iid, rel: bool | None = None,
             claim1: int | None = None, sclaim1: int | None = None,
             claim2: int | None = None, sclaim2: int | None = None):
    assi_id = sess.execute(
        insert(Assignment).values(assignment_scope_id=scope_id, user_id=uid, item_id=iid,
                                  annotation_scheme_id=SCHEME_ID, status='FULL').returning(Assignment.assignment_id)
    ).scalars().one()

    if rel is not None:
        sess.execute(insert(Annotation).values(
            assignment_id=assi_id, user_id=uid, item_id=iid, annotation_scheme_id=SCHEME_ID,
            key='rel', repeat=1, value_bool=rel, parent=None
        ))
    if claim1 is not None:
        parent_id = sess.execute(insert(Annotation).values(
            assignment_id=assi_id, user_id=uid, item_id=iid, annotation_scheme_id=SCHEME_ID,
            key='claim', repeat=1, value_int=claim1, parent=None
        ).returning(Annotation.annotation_id)).scalars().one()
        if sclaim1 is not None:
            sess.execute(insert(Annotation).values(
                assignment_id=assi_id, user_id=uid, item_id=iid, annotation_scheme_id=SCHEME_ID,
                key='sub-gw' if claim1 == 0 else 'sub-gg', repeat=1, value_int=sclaim1, parent=parent_id
            ))

    if claim2 is not None:
        parent_id = sess.execute(insert(Annotation).values(
            assignment_id=assi_id, user_id=uid, item_id=iid, annotation_scheme_id=SCHEME_ID,
            key='claim', repeat=2, value_int=claim2, parent=None
        ).returning(Annotation.annotation_id)).scalars().one()
        if sclaim1 is not None:
            sess.execute(insert(Annotation).values(
                assignment_id=assi_id, user_id=uid, item_id=iid, annotation_scheme_id=SCHEME_ID,
                key='sub-gw' if claim2 == 0 else 'sub-gg', repeat=1, value_int=sclaim2, parent=parent_id
            ))


with engine.session() as session:
    r = session.execute(text("SELECT item_id "
                             "FROM m2m_project_item "
                             "WHERE project_id = :project_id "
                             "LIMIT 10"),
                        {'project_id': PROJECT_ID})
    item_ids = r.scalars().all()

    scope = AssignmentScope(annotation_scheme_id=SCHEME_ID,
                            name='Test Scope',
                            description='programmatically generated set of annotations/assignments for testing')
    session.add(scope)
    session.commit()

    scope_id = scope.assignment_scope_id

    annotate(session, USER_IDS[0], item_ids[0],
             True, 0, 1, None, None)
    annotate(session, USER_IDS[0], item_ids[1],
             None, 0, 1, None, None)
    annotate(session, USER_IDS[0], item_ids[2],
             False, None, None, None, None)
    annotate(session, USER_IDS[0], item_ids[3],
             True, 1, 1, None, None)
    annotate(session, USER_IDS[0], item_ids[4],
             True, 1, 1, None, None)
    annotate(session, USER_IDS[0], item_ids[5],
             False, None, None, None, None)
    annotate(session, USER_IDS[0], item_ids[6],
             None, None, None, None, None)
    annotate(session, USER_IDS[0], item_ids[7],
             True, 0, 1, 1, 0)
    annotate(session, USER_IDS[0], item_ids[8],
             True, 0, 2, 1, 1)
    annotate(session, USER_IDS[0], item_ids[9],
             True, 0, 3, 1, 2)

    annotate(session, USER_IDS[1], item_ids[0],
             True, 0, 1, None, None)
    annotate(session, USER_IDS[1], item_ids[1],
             False, None, None, None, None)
    annotate(session, USER_IDS[1], item_ids[2],
             True, 0, 1, None, None)
    annotate(session, USER_IDS[1], item_ids[3],
             None, 1, 1, None, None)
    annotate(session, USER_IDS[1], item_ids[4],
             True, 1, 1, None, None)
    annotate(session, USER_IDS[1], item_ids[5],
             False, None, None, None, None)
    annotate(session, USER_IDS[1], item_ids[6],
             True, 0, 1, 1, 0)
    annotate(session, USER_IDS[1], item_ids[7],
             False, None, None, None, None)
    annotate(session, USER_IDS[1], item_ids[8],
             True, 0, 2, 1, 1)
    annotate(session, USER_IDS[1], item_ids[9],
             True, 0, 3, 1, 2)

    annotate(session, USER_IDS[2], item_ids[0],
             False, None, None, None, None)
    annotate(session, USER_IDS[2], item_ids[1],
             True, 1, 3, 0, 1)
    annotate(session, USER_IDS[2], item_ids[2],
             True, 1, 0, None, None)
    annotate(session, USER_IDS[2], item_ids[3],
             True, 0, 1, None, None)
    annotate(session, USER_IDS[2], item_ids[4],
             False, None, None, None, None)
    annotate(session, USER_IDS[2], item_ids[5],
             False, None, None, None, None)
    annotate(session, USER_IDS[2], item_ids[6],
             True, 1, 1, 1, 0)
    annotate(session, USER_IDS[2], item_ids[7],
             None, None, None, None, None)
    annotate(session, USER_IDS[2], item_ids[8],
             True, 0, 2, 1, 1)
    annotate(session, USER_IDS[2], item_ids[9],
             True, 0, 3, 1, 2)


