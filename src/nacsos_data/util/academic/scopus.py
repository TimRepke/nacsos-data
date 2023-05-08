import logging
import csv
import re
import uuid
from typing import Generator

from ...models.items import AcademicItemModel
from ...models.items.academic import AcademicAuthorModel
from .duplicate import str_to_title_slug

logger = logging.getLogger('nacsos_data.util.academic.scopus')


def _get(obj: dict[str, str], field: str) -> str | None:
    """
    Helper function to get value from dictionary and only return not-None if it's also a non-empty string
    :param obj:
    :param field:
    :return:
    """
    val = obj.get(field)
    if val is not None and len(val) > 0:
        return val
    return None


FULL_AUTHOR = re.compile(r'([^(]+) \(([^)]+)\)')


def _parse_authors(row: dict[str, str]) -> list[AcademicAuthorModel] | None:
    fn_authors: list[str] | None = None
    fn_author_ids: list[str] | None = None
    if _get(row, 'Author full names') is not None:
        try:
            tmp = [
                m.groups()
                for m in [
                    FULL_AUTHOR.match(s.strip())
                    for s in _get(row, 'Author full names').split(';')  # type: ignore[union-attr]
                ]
                if m is not None
            ]

            fn_authors = [t[0] for t in tmp]
            fn_author_ids = [t[1] for t in tmp]
        except Exception:
            # ignore errors, yolo
            pass

    authors: list[str] | None = None
    author_ids: list[str] | None = None
    if _get(row, 'Authors') is not None:
        authors = [s.strip() for s in _get(row, 'Authors').split(';')]  # type: ignore[union-attr]
        if _get(row, 'Author(s) ID') is not None:
            author_ids = [s.strip() for s in _get(row, 'Author(s) ID').split(';')]  # type: ignore[union-attr]

    # in case we have all the information, let's use it
    if (fn_authors is not None
            and authors is not None
            and fn_author_ids is not None
            and len(fn_authors) == len(authors)):
        return [
            AcademicAuthorModel(name=author,
                                scopus_id=fn_author_ids[ai],
                                surname_initials=authors[ai])
            for ai, author in enumerate(fn_authors)
        ]

    # try the next best thing: full names with IDs!
    if fn_authors is not None and fn_author_ids is not None and len(fn_authors) == len(fn_author_ids):
        return [
            AcademicAuthorModel(name=author,
                                scopus_id=fn_author_ids[ai])
            for ai, author in enumerate(fn_authors)
        ]

    # do we at least have the short names with IDs?
    if authors is not None and author_ids is not None and len(authors) == len(author_ids):
        return [
            AcademicAuthorModel(name=author,
                                scopus_id=author_ids[ai])
            for ai, author in enumerate(authors)
        ]

    # getting, desperate, do we have full names?
    if fn_authors is not None:
        return [AcademicAuthorModel(name=author) for author in fn_authors]

    # last resort, do we have short names?
    if authors is not None:
        return [AcademicAuthorModel(name=author) for author in authors]

    # I give up...
    return None


def read_scopus_file(filepath: str,
                     project_id: str | uuid.UUID | None = None) -> Generator[AcademicItemModel, None, None]:
    """
    This function will read a scopus csv line by line and return a generator of parsed `AcademicItemModel`.

    :param project_id:
    :param filepath:
    :return:
    """
    with open(filepath, mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            doi: str | None = _get(row, 'DOI')
            if doi is not None:
                doi = doi.replace('https://doi.org/', '')

            meta_info: dict[str, str | list[str]] = {
                key: _get(row, key)  # type: ignore[misc]
                for key in ['Volume', 'Issue', 'Art. No.', 'Page start',
                            'Page end', 'Page count', 'Cited by', 'Editors',
                            'Publisher', 'ISSN', 'ISBN', 'CODEN',
                            'Language of Original Document',
                            'Document Type', 'Publication Stage', 'Open Access']
                if _get(row, key) is not None
            }

            # Unfortunately, there's no good way to associate affiliations to authors,
            # so at least remember all unique affiliations in the meta-data
            if _get(row, 'Affiliations') is not None:
                meta_info['affiliations'] = list(set([
                    aff.strip()
                    for aff in _get(row, 'Affiliations').split(';')  # type: ignore[union-attr]
                ]))

            keywords = None
            if _get(row, 'Author Keywords') is not None:
                keywords = [
                    kw.strip()
                    for kw in _get(row, 'Author Keywords').split(';')  # type: ignore[union-attr]
                    if len(kw.strip()) > 0
                ]

            title = _get(row, 'Titles')
            title_slug = str_to_title_slug(title)
            authors = _parse_authors(row)

            py: int | None = None
            if _get(row, 'Year') is not None:
                py = int(_get(row, 'Year'))  # type: ignore[union-attr,arg-type]

            doc = AcademicItemModel(project_id=project_id,
                                    scopus_id=_get(row, 'EID'),
                                    doi=doi,
                                    title=title,
                                    text=_get(row, 'Abstract'),
                                    publication_year=py,
                                    keywords=keywords,
                                    pubmed_id=_get(row, 'PubMed ID'),
                                    title_slug=title_slug,
                                    source=_get(row, 'Source title'),
                                    authors=authors,
                                    meta=meta_info)
            yield doc
