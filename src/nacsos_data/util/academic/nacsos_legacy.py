import logging
import datetime
import uuid
from collections import defaultdict
from typing import Any

from ...models.annotations import AssignmentModel, AssignmentStatus, AnnotationModel
from ...models.items.academic import AcademicAuthorModel, AffiliationModel, AcademicItemModel
from ..errors import NotFoundError
from .duplicate import get_title_slug

logger = logging.getLogger('nacsos_data.util.academic.nacsos_legacy')


def _convert_authors(authors: list[object]) -> list[AcademicAuthorModel] | None:
    aggregate = defaultdict(list)

    # iterate list of authors and aggregate by "position"
    # authors with the same position are assumed to be identical with varying affiliation
    for author in authors:
        aggregate[author.position].append(author.__dict__)  # type: ignore[arg-type,attr-defined]

    ret = []

    for key in sorted(aggregate.keys()):
        # Get all names from all possible fields for this author, so we have a fallback to pick
        # Sort by length of string, so the longest name will be the last in the list
        names = list(sorted([
            a.get(src)
            for a in aggregate[key]
            for src in ['AU', 'AF', 'surname', 'initials']
            if a.get(src) is not None and len(a.get(src)) > 0  # type: ignore[arg-type]
        ], key=lambda n: len(n)))  # type: ignore[arg-type]

        # Get all author fields in this aggregate, keep non-empty ones
        name_initials = [
            a.get('AU')
            for a in aggregate[key]
            if a.get('AU') is not None and len(a.get('AU')) > 0  # type: ignore[arg-type]
        ]

        # Get all non-empty affiliations
        affiliations = [
            AffiliationModel(name=a.get('institution'))  # type: ignore[arg-type]
            for a in aggregate[key]
            if a.get('institution') is not None and len(a.get('institution')) > 0  # type: ignore[arg-type]
        ]

        # If we have at least one non-empty name for this author, add them
        if len(names) > 0:
            ret.append(AcademicAuthorModel(
                name=names[-1],  # type: ignore[arg-type]
                surname_initials=name_initials[0] if len(name_initials) > 0 else None,
                affiliations=affiliations if len(affiliations) > 0 else None
            ))

    if len(ret) > 0:
        return ret

    return None


def fetch_nacsos_legacy_doc(doc_id: int, project_id: str | None, models: Any) -> AcademicItemModel | None:
    """
    This function can be used to fetch a document from NACSOS-legacy and convert it to an `AcademicItemModel`.
    We assume, that this function will only be used on the VM and the necessary Django setup is in its context.

    Example setup:
    ```python
    import sys
    import os

    # Add NACSOS codebase to runtime (configuration and scoping library)
    sys.path.append('/var/www/nacsos1/tmv/BasicBrowser')
    os.environ['DJANGO_SETTINGS_MODULE'] = 'BasicBrowser.settings'
    os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

    import django
    from django.conf import settings as django_settings
    from BasicBrowser import settings as nacsos_settings

    if django_settings.configured:
        print('Loading settings from pre-configured Django')
        django_settings.LOGGING = None
        django_settings.LOGGING_CONFIG = None
        django_settings.FORCE_SCRIPT_NAME = None
    else:
        print('Loading settings from file')
        nacsos_settings.LOGGING = None
        nacsos_settings.LOGGING_CONFIG = None
        nacsos_settings.FORCE_SCRIPT_NAME = None
        django_settings.configure(default_settings=nacsos_settings, DEBUG=True)
    django.setup(set_prefix=False)

    from scoping import models as scoping_models
    ```

    Example usage:
    ```python
    from nacsos_data.util.academic.nacsos_legacy import fetch_nacsos_legacy_doc

    # project_id for where to put this in nacsos2 (optional here, can also be set afterwards)
    project_id = '...'

    # magically comes from somewhere
    nacsos1_doc_ids = [...]

    # we will collect items here
    nacsos1_academic_items = []
    for doc_id in nacsos1_doc_ids:
        try:
            item = fetch_nacsos_legacy_doc(doc_id=doc_id, project_id=project_id, models=scoping_models)
            nacsos1_academic_items.append(item)
        except NotFoundError as e:
            print(e)
    ```

    :param models: from scoping import models
    :param doc_id:
    :param project_id:
    :return:
    """
    document = models.Doc.objects.get(pk=doc_id)
    wos_article = models.WoSArticle.objects.get(pk=doc_id)

    if document is None or wos_article is None:
        raise NotFoundError(f'Doc or WosArticle missing for {doc_id}')

    # Parse list of authors
    doc_authors = _convert_authors(document.authorlist())

    item = AcademicItemModel(project_id=project_id, authors=doc_authors)
    meta = {}

    # Check if we can get a DOI
    if wos_article.di and len(wos_article.di) > 0:
        item.doi = wos_article.di

    # Test IDs to see if one looks like a WoS or Scopus ID
    if document.UT.UT is not None and document.UT.UT[:3].lower() == 'wos':
        item.wos_id = document.UT.UT
    if document.UT.sid is not None and document.UT.sid[:3].lower() == 'wos':
        item.wos_id = document.UT.sid
    if document.UT.UT is not None and document.UT.UT[:4].lower() == '2-s2':
        item.scopus_id = document.UT.UT
    if document.UT.sid is not None and document.UT.sid[:4].lower() == '2-s2':
        item.scopus_id = document.UT.sid

    # Find best match for the title
    if document.title and len(document.title) > 0:
        item.title = document.title
    elif wos_article.ti and len(wos_article.ti) > 0:
        item.title = wos_article.ti
    item.title_slug = get_title_slug(item)

    # Find best match for the publication year
    item.publication_year = document.PY
    if item.publication_year is None:
        item.publication_year = wos_article.py

    # Find best match for the Journal source field
    if document.journal is not None and document.journal.fulltext is not None and len(document.journal.fulltext) > 0:
        item.source = document.journal.fulltext  # JournalAbbrev
        meta['journal'] = document.journal.fulltext
    elif wos_article.ji is not None and len(wos_article.ji) > 0:
        item.source = wos_article.ji  # ISO Source Abbreviation
        meta['ji'] = wos_article.ji
    elif wos_article.j9 is not None and len(wos_article.j9) > 0:
        item.source = wos_article.j9  # 29-Character Source Abbreviation
        meta['j9'] = wos_article.j9
    elif wos_article.so is not None and len(wos_article.so) > 0:
        item.source = wos_article.so  # Publication Name
        meta['so'] = wos_article.so
    elif wos_article.se is not None and len(wos_article.se) > 0:
        item.source = wos_article.se  # Book Series Title
        meta['se'] = wos_article.se

    # Find best match for the abstract
    if wos_article.ab is not None and len(wos_article.ab) > 0:
        item.text = wos_article.ab
    elif document.content is not None and len(document.content) > 0:
        item.text = document.content

    # Gather all possible keywords
    item.keywords = []
    if wos_article.de is not None and len(wos_article.de) > 0:
        # Author Keywords
        item.keywords = [kw.strip() for kw in wos_article.de.split(';')]
    if wos_article.kwp is not None and len(wos_article.kwp) > 0:
        # Keywords Plus
        item.keywords += [kw.strip() for kw in wos_article.kwp.split(';')]
    if len(item.keywords) == 0:
        item.keywords = None

    # Populate meta field with all we can get
    if wos_article.bn is not None and len(wos_article.bn) > 0:
        meta['isbn'] = wos_article.bn  # WoS ISBN
    if wos_article.c1 is not None and len(wos_article.c1) > 0:
        meta['author_addr'] = wos_article.c1  # WoS Author Address
    if wos_article.cl is not None and len(wos_article.cl) > 0:
        meta['conf_loc'] = wos_article.cl  # WoS Conference Location
    if wos_article.ct is not None and len(wos_article.ct) > 0:
        meta['conf_tit'] = wos_article.ct  # WoS Conference Title
    if wos_article.dt is not None and len(wos_article.dt) > 0:
        meta['doc_type_wos'] = wos_article.dt  # WoS Document Type
    if wos_article.dt is not None and len(wos_article.dt) > 0:
        meta['doc_type_wos'] = wos_article.dt  # WoS Document Type
    if wos_article.fu is not None and len(wos_article.fu) > 0:
        meta['fund_num'] = wos_article.fu  # WoS Funding Agency and Grant Number
    if wos_article.fx is not None and len(wos_article.fx) > 0:
        meta['fund_txt'] = wos_article.fx  # WoS Funding Text
    if wos_article.iss is not None and len(wos_article.iss) > 0:
        meta['issue'] = wos_article.iss  # WoS Issue
    if wos_article.la is not None and len(wos_article.la) > 0:
        meta['lang'] = wos_article.la  # WoS Language
    if wos_article.pd is not None and len(wos_article.pd) > 0:
        meta['date'] = wos_article.pd  # WoS Publication date (might be year, month, day;  some of them)
    if wos_article.pu is not None and len(wos_article.pu) > 0:
        meta['publisher'] = wos_article.pu  # WoS Publisher
    if wos_article.sc is not None and len(wos_article.sc) > 0:
        meta['subj'] = wos_article.sc  # WoS Subject Category
    if wos_article.si is not None and len(wos_article.si) > 0:
        meta['spec_issue'] = wos_article.si  # WoS Special Issue
    if wos_article.sn is not None and len(wos_article.sn) > 0:
        meta['issn'] = wos_article.sn  # WoS ISSN
    if wos_article.vl is not None and len(wos_article.vl) > 0:
        meta['vol'] = wos_article.vl  # WoS Volume
    if wos_article.wc is not None and len(wos_article.wc) > 0:
        meta['wos_cat'] = wos_article.wc  # WoS Web of Science Category

    # clear meta entry if we didn't pick up anything
    if len(meta) > 0:
        item.meta = {'nacsos1': meta}

    return item


def read_nacsos1_annotations(
        query_id: int,
        models: Any,
        assignment_scope_id: str | uuid.UUID,
        annotation_scheme_id: str | uuid.UUID,
        user_map: dict[int, str | uuid.UUID],
        label_map: dict[int, str],
        relevance_key: str | None,
        item_map: dict[int, str | uuid.UUID]
) -> tuple[list[AssignmentModel], list[AnnotationModel]]:
    """
    We'll assume you are running this on the VM and have the necessary connections open.
    This function will read all `DocOwnership` and `DocUserCat` entries related to `query_id`.
    Based on that, it will try to make a best effort of creating respective counterparts
    for nacsos2 (`Assignment` and `Annotation`).
    Note, that all labels are assumed to be binary (except relevance: single-choice).

    We also assume that you created a matching annotation scheme and assignment scope already.
    The scheme should not include any nesting. If you do, you have to do some post-processing to add the
    necessary parent ids in the annotations.
    This method does not save anything, that's your job after double-checking the results.
    This is not considered to be a bullet-proof solution, results may vary between projects.

    Creating an `Annotation` for relevance if `relevance_key` is not None.
    value_int will be 0=no, 1=maybe, 2=yes

    :param query_id: nacsos1 query_id to get data from (assumed to be root query covering all annotations and documents)
    :param models: Django models (see fetch_nacsos_legacy_doc() for more information)
    :param relevance_key: `Annotation.key` to use for the relevance Annotation
    :param assignment_scope_id: scope_id to refer the new objects to
    :param annotation_scheme_id: scheme_id to refer the new objects to
    :param user_map: mapping from nacsos1 user_id to nacsos2 user_id
    :param label_map: mapping from nacsos1 `category_id` to nacsos2 `Annotation.key`
    :param item_map: mapping from nacsos1 doc_id to nacsos2 item_id
    :return:
    """
    q = models.Query.objects.get(pk=query_id)
    p = q.project

    all_categories = set(models.Category.objects.filter(project=p).values_list('id', flat=True))

    # all unique doc_ids sorted in order of assignment
    doc_ids = [
        doid['doc_id']
        for doid in sorted(list(models.DocOwnership.objects
                                .filter(query__project=p, doc__query=q, relevant__gt=0)
                                .distinct('doc_id')
                                .values('doc_id', 'id')),
                           key=lambda x: x['id'])  # type: ignore[arg-type, no-any-return]
    ]

    assignments_new = []
    annotations_new = []

    for order, doc_id in enumerate(doc_ids):
        if doc_id not in item_map:
            logger.warning(f'Encountered doc_id={doc_id} which I do not know about. Ignoring!')

        # Assignments
        assignments = models.DocOwnership.objects.filter(doc_id=doc_id, query__project=p)

        for assignment in assignments:
            doc_user_cats = models.DocUserCat.objects.filter(doc_id=doc_id, user=assignment.user, category__project=p)
            pos_categories = set(doc_user_cats.values_list('category_id', flat=True))
            neg_categories = all_categories - pos_categories

            # User `assignment.user_id`
            # thinks that document `assignment.doc_id`
            # has relevance `assignment.relevant` (0=unrated, 1=yes, 2=no, 3=maybe)
            # and is about categories `pos_categories`
            # but not about categories `neg_categories`

            assignment_new = AssignmentModel(
                # creation date in nacsos1: assignment['date']
                assignment_id=uuid.uuid4(),
                assignment_scope_id=assignment_scope_id,
                user_id=user_map[assignment.user_id],
                item_id=item_map[assignment.doc_id],
                annotation_scheme_id=annotation_scheme_id,
                status=AssignmentStatus.FULL,
                order=order)
            assignments_new.append(assignment_new)

            # assignment: ('id', 'doc_id', 'docpar_id', 'utterance_id', 'tweet_id', 'document_linked',
            #              'utterance_linked', 'title_only', 'full_text', 'project_id', 'user_id', 'query_id', 'tag_id',
            #              'order', 'relevant', 'date', 'start', 'finish')
            # doc_user_cats: list[('id', 'doc_id', 'docpar_id', 'utterance_id', 'tweet_id', 'document_linked',
            #                      'utterance_linked', 'title_only', 'full_text', 'project_id', 'user_id', 'query_id',
            #                      'tag_id', 'order', 'relevant', 'date', 'start', 'finish')]

            if relevance_key is not None and 0 < assignment.relevant < 4:
                annotations_new.append(
                    AnnotationModel(
                        annotation_id=uuid.uuid4(),
                        # we don't really know, so let's take the time the assignment was finished
                        time_created=assignment.finish,
                        time_updated=datetime.datetime.now(),
                        assignment_id=assignment_new.assignment_id,  # type: ignore[arg-type]
                        user_id=user_map[assignment.user_id],
                        item_id=item_map[assignment.doc_id],
                        annotation_scheme_id=annotation_scheme_id,
                        key=relevance_key,
                        repeat=1,
                        parent=None,
                        value_bool=None,
                        # (0=unrated, 1=yes, 2=no, 3=maybe) -> (0=no, 1=maybe, 2=yes)
                        value_int={0: None, 1: 2, 2: 0, 3: 1}[assignment.relevant],
                        value_float=None,
                        value_str=None,
                        multi_int=None,
                        text_offset_start=None,
                        text_offset_stop=None
                    )
                )

                for cat in (doc_user_cats or []):
                    annotations_new.append(
                        AnnotationModel(
                            annotation_id=uuid.uuid4(),
                            time_created=cat.time,
                            time_updated=datetime.datetime.now(),
                            assignment_id=assignment_new.assignment_id,  # type: ignore[arg-type]
                            user_id=user_map[cat.user_id],
                            item_id=item_map[cat.doc_id],
                            annotation_scheme_id=annotation_scheme_id,
                            key=label_map[cat.category_id],
                            repeat=1,
                            parent=None,
                            value_bool=True,
                            value_int=None,
                            value_float=None,
                            value_str=None,
                            multi_int=None,
                            text_offset_start=None,
                            text_offset_stop=None
                        )
                    )
                for neg_cat in neg_categories:
                    annotations_new.append(
                        AnnotationModel(
                            annotation_id=uuid.uuid4(),
                            # we don't really know, so let's take the time the assignment was finished
                            time_created=assignment.finish,
                            time_updated=datetime.datetime.now(),
                            assignment_id=assignment_new.assignment_id,  # type: ignore[arg-type]
                            user_id=user_map[assignment.user_id],
                            item_id=item_map[assignment.doc_id],
                            annotation_scheme_id=annotation_scheme_id,
                            key=label_map[neg_cat],
                            repeat=1,
                            parent=None,
                            value_bool=False,
                            value_int=None,
                            value_float=None,
                            value_str=None,
                            multi_int=None,
                            text_offset_start=None,
                            text_offset_stop=None
                        )
                    )
    return assignments_new, annotations_new
