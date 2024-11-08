from sqlalchemy import text, insert
from nacsos_data.db.connection import get_engine
from nacsos_data.db.schemas import Assignment, AssignmentScope, Annotation

PROJECT_ID = '3e87c64e-115b-42cb-8992-b266700eebd1'
SCHEME_ID = 'c6d3f9a2-8465-42ea-a228-cbd9c00f9222'
USER_IDS = ['562fc779-3541-4796-a9b8-d4da580892cb',
            'a4b6c733-ea3a-43ce-a498-084c10f91110',
            '24b642d5-26eb-4a05-858b-2244b2b2542a']

engine = get_engine(conf_file='../../nacsos-core/config/local.env')


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
                             "FROM item "
                             "WHERE project_id = :project_id "
                             "LIMIT 10"),
                        {'project_id': PROJECT_ID})
    item_ids = r.scalars().all()
    print(item_ids)

    scope = AssignmentScope(annotation_scheme_id=SCHEME_ID,
                            name='Test Scope 2',
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


