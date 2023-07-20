import datetime
import random

import typer
from sqlalchemy import insert
from sqlalchemy.orm import Session

from nacsos_data.db.connection import get_engine
from nacsos_data.db.crud.users import get_password_hash
from nacsos_data.db.schemas import \
    Assignment, \
    AssignmentScope, \
    Annotation, \
    AnnotationScheme, \
    BotAnnotationMetaData, \
    BotAnnotation, \
    Project, \
    ProjectPermissions, \
    ItemType, \
    TwitterItem, \
    AcademicItem, \
    GenericItem, \
    User, \
    m2m_import_item_table, \
    Import
from nacsos_data.models.annotations import AnnotationSchemeLabel, AnnotationSchemeLabelChoice, AssignmentStatus
from nacsos_data.models.bot_annotations import BotKind
from nacsos_data.models.imports import ImportType, M2MImportItemType
from nacsos_data.models.items.academic import AcademicAuthorModel, AffiliationModel


def main(
        config: str = '../../nacsos-core/config/testing.env',
        p_type: ItemType = ItemType.twitter,
        init_schema: bool = True,
        clear_schema: bool = True
):
    engine = get_engine(conf_file=config)

    if clear_schema:
        from nacsos_data.db.base_class import Base
        Base.metadata.drop_all(engine.engine)

    if init_schema:
        from nacsos_data.db.base_class import Base
        Base.metadata.create_all(engine.engine)

    with engine.session() as session:  # type: Session

        users = [
            User(username='user1', email='a@b.de', full_name='U 1', affiliation='BASF',
                 is_superuser=True, is_active=True, password=get_password_hash('1234')),
            User(username='user2', email='a@b.eu', full_name='U 2', affiliation='SHELL',
                 is_superuser=True, is_active=True, password=get_password_hash('1234')),
        ]
        session.add_all(users)

        # create project
        project = Project(name='Dummy Project', description='Description for dummy project', type=p_type)
        session.add(project)
        session.commit()

        # create permissions
        perms: list[ProjectPermissions] = [
            ProjectPermissions(project_id=project.project_id, user_id=user.user_id, owner=True)
            for user in users
        ]
        session.add_all(perms)

        scheme_obj = [AnnotationSchemeLabel(name='Dummy label 1', key='d1', max_repeat=1, required=True, kind='single',
                                            choices=[AnnotationSchemeLabelChoice(name='d1_1', value=0).model_dump(),
                                                     AnnotationSchemeLabelChoice(name='d1_2', value=1).model_dump()]).model_dump(),
                      AnnotationSchemeLabel(name='Dummy label 2', key='d2', max_repeat=1, required=True, kind='single',
                                            choices=[AnnotationSchemeLabelChoice(name='d2_1', value=0).model_dump(),
                                                     AnnotationSchemeLabelChoice(name='d2_2', value=1).model_dump()]).model_dump()]
        scheme = AnnotationScheme(project_id=project.project_id,
                                  name='Dummy scheme', description='Description for dummy scheme',
                                  labels=scheme_obj)
        session.add(scheme)
        session.commit()

        scope = AssignmentScope(annotation_scheme_id=scheme.annotation_scheme_id,
                                name='Dummy assignment scope', description='Description for dummy assignment scope')
        session.add(scope)
        session.commit()

        if p_type == ItemType.twitter:
            items = [
                TwitterItem(project_id=project.project_id, text='Voll der tweet!', twitter_id=5, twitter_author_id=10,
                            created_at=datetime.datetime(2022, 6, 5, 4, 3, 2), language='en',
                            conversation_id=100, retweet_count=4, reply_count=3,
                            like_count=2, quote_count=1),
                TwitterItem(project_id=project.project_id, text='Toll!', twitter_id=6, twitter_author_id=12,
                            created_at=datetime.datetime(2020, 6, 5, 4, 3, 2), language='en',
                            conversation_id=102, retweet_count=4, reply_count=3,
                            like_count=2, quote_count=1),
                TwitterItem(project_id=project.project_id, text='Mega!', twitter_id=7, twitter_author_id=11,
                            created_at=datetime.datetime(2020, 6, 4, 4, 3, 2), language='en',
                            conversation_id=102, retweet_count=4, reply_count=3,
                            like_count=2, quote_count=1),
                TwitterItem(project_id=project.project_id, text='Knorke!', twitter_id=8, twitter_author_id=10,
                            created_at=datetime.datetime(2021, 6, 5, 4, 3, 2), language='en',
                            conversation_id=103, retweet_count=4, reply_count=3,
                            like_count=2, quote_count=1)
            ]
        elif p_type == ItemType.academic:
            items = [
                AcademicItem(project_id=project.project_id,
                             text='We introduce Spatio-Temporal Momentum strategies, a class of models that unify both time-series and cross-sectional momentum strategies by trading assets based on their cross-sectional momentum features over time. ',
                             title='Spatio-Temporal Momentum', title_slug='spaciotemporalmomentum',
                             publication_year=2023,
                             source='arXiv', keywords=['kw 1', 'kw 2'],
                             authors=[
                                 AcademicAuthorModel(name='Wee Ling', orcid='https://orcid.org/0000-0001-9661-6325').model_dump(),
                                 AcademicAuthorModel(name='Stephen Roberts',
                                                     affiliations=[AffiliationModel(name='Oxford', country='UK').model_dump()]).model_dump()],
                             meta={'meta_str': 'test', 'meta_int': 10, '_meta_hidden': 'secret'},
                             doi='10.48550/arXiv.2302.10175'),
                AcademicItem(project_id=project.project_id,
                             text='With generative models proliferating at a rapid rate, there is a growing need for general purpose fake image detectors. In this work, we first show that the existing paradigm, ',
                             title='Towards Universal Fake Image', title_slug='towardsuniversialfakeimages',
                             publication_year=2022,
                             source='arXiv', keywords=['kw 2', 'kw 3'],
                             authors=[
                                 AcademicAuthorModel(name='Utkarsh Ojha', orcid='https://orcid.org/0000-0001-9661-6325').model_dump(),
                                 AcademicAuthorModel(name='Yong Jae Lee',
                                                     affiliations=[AffiliationModel(name='Wisconsin-Madison', country='USA').model_dump()]).model_dump()],
                             doi='10.48550/arXiv.2302.10174'),
                AcademicItem(project_id=project.project_id,
                             text='Exploiting robots for activities in human-shared environments, whether warehouses, shopping centres or hospitals, calls for such robots to understand the underlying physical interactions between nearby agents and objects. In particular, ',
                             title='Causal Discovery of Dynamic Models for Predicting Human Spatial Interactions',
                             title_slug='causaldiscovery',
                             publication_year=1980,
                             source='arXiv', keywords=['kw 4', 'kw 5'],
                             authors=[AcademicAuthorModel(name='Luca Castri').model_dump()],
                             doi='10.48550/arXiv.2210.16535'),
                AcademicItem(project_id=project.project_id,
                             text='Modern policy optimization methods in applied reinforcement learning, such as Trust Region Policy Optimization and Policy Mirror Descent, are often based on the policy gradient framework. While theoretical guarantees have been established for this class of algorithms, particularly in the tabular setting, the use of a general parametrization scheme remains mostly unjustified.',
                             title='A Novel Framework for Policy Mirror Descent with General Parametrization and Linear Convergence',
                             title_slug='anovelframeworkforpolicymirror',
                             publication_year=2023,
                             source='arXiv', keywords=['kw 1', 'kw 2'],
                             authors=[
                                 AcademicAuthorModel(name='Carlo Alfano').model_dump(),
                                 AcademicAuthorModel(name='Rui Yuan').model_dump(),
                                 AcademicAuthorModel(name='Patrick Rebeshini').model_dump()],
                             doi='10.48550/arXiv.2301.13139'),
            ]
        elif p_type == ItemType.generic:
            raise NotImplementedError()
        else:
            raise NotImplementedError()

        session.add_all(items)
        session.commit()

        imp1 = Import(user_id=users[0].user_id, project_id=project.project_id,
                      name='Dummy import 1', description='Description for dummy import 1',
                      type=ImportType.script,
                      time_created=datetime.datetime.now(),
                      time_started=datetime.datetime.now(),
                      time_finished=datetime.datetime.now())
        session.add(imp1)

        imp2 = Import(user_id=users[0].user_id, project_id=project.project_id,
                      name='Dummy import 2', description='Description for dummy import 2',
                      type=ImportType.script,
                      time_created=datetime.datetime.now(),
                      time_started=datetime.datetime.now(),
                      time_finished=datetime.datetime.now())
        session.add(imp2)
        session.commit()

        bam = BotAnnotationMetaData(name='Dummy bot annotation', kind=BotKind.SCRIPT, project_id=project.project_id)
        session.add(bam)
        session.commit()

        assignments = []
        annotations = []
        bot_annotations = []
        for i, item in enumerate(items):
            session.execute(insert(m2m_import_item_table)
                            .values(item_id=item.item_id, import_id=imp1.import_id, type=M2MImportItemType.explicit))
            if (i % 2) == 0:
                session.execute(insert(m2m_import_item_table)
                                .values(item_id=item.item_id, import_id=imp2.import_id,
                                        type=M2MImportItemType.explicit))

            for user in users:
                assi = Assignment(user_id=user.user_id, item_id=item.item_id, order=len(assignments),
                                  status=AssignmentStatus.FULL,
                                  annotation_scheme_id=scheme.annotation_scheme_id,
                                  assignment_scope_id=scope.assignment_scope_id)
                session.add(assi)
                session.commit()
                assignments.append(assi)

                annotations.append(Annotation(assignment_id=assi.assignment_id,
                                              annotation_scheme_id=scheme.annotation_scheme_id,
                                              user_id=user.user_id, item_id=item.item_id,
                                              key='d1', value_int=random.randint(0, 1), repeat=1))
                annotations.append(Annotation(assignment_id=assi.assignment_id,
                                              annotation_scheme_id=scheme.annotation_scheme_id,
                                              user_id=user.user_id, item_id=item.item_id,
                                              key='d2', value_int=random.randint(0, 1), repeat=1))

            bot_annotations.append(BotAnnotation(bot_annotation_metadata_id=bam.bot_annotation_metadata_id,
                                                 item_id=item.item_id, key='dummy',
                                                 value_int=random.randint(0, 10)))

        session.add_all(annotations)
        session.commit()


if __name__ == '__main__':
    typer.run(main)
