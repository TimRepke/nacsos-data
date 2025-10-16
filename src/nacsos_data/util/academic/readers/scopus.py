import re
import csv
import sys
import uuid
import logging
from typing import Generator

import rispy

from ....models.items import AcademicItemModel
from ....models.items.academic import AcademicAuthorModel
from ..duplicate import str_to_title_slug
from ..clean import clear_empty

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


def read_scopus_csv_file(filepath: str,
                         project_id: str | uuid.UUID | None = None) -> Generator[AcademicItemModel, None, None]:
    """
    This function will read a scopus csv line by line and return a generator of parsed `AcademicItemModel`.

    :param project_id:
    :param filepath:
    :return:
    """

    with open(filepath, mode='r', newline='') as csvfile:
        csv.field_size_limit(sys.maxsize)
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
            if title is None:
                title = _get(row, 'Title')
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
                                    meta=clear_empty(meta_info))
            yield doc


# Based on
# https://service.elsevier.com/app/answers/detail/a_id/14805/supporthub/scopus/~/what-ris-format-mapping-does-scopus-use-when-exporting-results-when-exchanging/
PUBMED_RIS_KEYS = {
    # Abbreviated source title
    # 'JA': 'abbreviated_source_title',  # Old Scopus RIS tag
    'J2': 'abbreviated_source_title',
    # Abstract
    'AB': 'abstract',
    # Affiliations
    'AD': 'affiliations',
    # Article number
    # 'N1': 'article_number',  # Old Scopus RIS tag
    'C7': 'article_number',
    # Article title
    # 'T1': 'article_title',  # Old Scopus RIS tag
    'TI': 'article_title',
    # Authors
    'AU': 'authors',
    # Chemical name and CAS registry number
    # 'N1': 'note',  # "Chemicals/CAS:"
    # Cited by count
    # 'N1': 'note',  # "Cited by:"
    # Conference Code
    # 'N1': 'note',  # "Conference code:"
    # CODEN
    # 'N1': 'note',  # "CODEN:"
    # Correspondence name
    # 'N1': 'note',  # "Correspondence address:"
    # DOI
    # 'N1': 'doi',  # Old Scopus RIS tag
    'DO': 'doi',
    # Editor
    'A2': 'editors',
    # Export date
    # 'N1': 'note',  # "Export date:"
    # Funding Details
    # 'N1': 'note',
    # ISSN/ISBN/EISSN
    'SN': 'issn',
    # Issue
    'IS': 'issue',
    # Keywords
    'KW': 'keywords',
    # Language
    # 'N1': 'language',  # Old Scopus RIS tag
    'LA': 'language',
    # First page
    'SP': 'first_page',
    # Last page
    'EP': 'last_page',
    # Conference name
    # 'T': 'conference_name',  # Old Scopus RIS tag
    'T2': 'conference_name',
    # Conference date
    'Y2': 'conference_date',
    # Conference Location
    'CY': 'conference_location',
    # Manufacturers
    # 'N1': 'note',
    # PMID/PMCID
    # 'N1': 'pmid',  # Old Scopus RIS tag
    'C2': 'pmid',
    # Proceedings title
    # 'N1': 'proceedings_title',  # Old Scopus RIS tag
    'C3': 'proceedings_title',
    # Publication year
    'PY': 'publication_year',
    # Publisher
    'PB': 'publisher',
    # References
    # 'N1': 'note',  # "References:"
    # Scopus database
    # 'N1': 'scopus_db',  # Old Scopus RIS tag
    'DB': 'scopus_db',
    # Scopus URL
    'UR': 'scopus_url',
    # Second article title
    # 'T2': 'second_article_title',  # Old Scopus RIS tag
    'ST': 'second_article_title',
    # Sequence database accession number
    # 'N1': 'note',  # Old Scopus RIS tag
    # Source title
    # 'JF': 'source_title',  # Old Scopus RIS tag
    # 'T2': 'source_title',
    # Source type
    'TY': 'type_of_reference',
    # Document type
    'M3': 'document_type',
    # Conference sponsors
    # 'N1': 'conference_sponsors',  # Old Scopus RIS tag
    'A4': 'conference_sponsors',
    # Tradenames
    # 'N1': 'note',  # "Tradenames:"
    # Volume
    'VL': 'volume',
    # End tag
    'ER': 'end_of_reference',
    'UK': 'unknown_tag',
}


class ScopusParser(rispy.RisParser):
    START_TAG = "TY"
    PATTERN = r"^[A-Z][A-Z0-9]  - |^ER  -\s*$"

    DEFAULT_LIST_TAGS = [
        'A1',
        'A2',
        'A3',
        'A4',
        'KW',
        'N1',
        'UR',
        'AD',
        'AU',
    ]

    DEFAULT_MAPPING = {**rispy.config.TAG_KEY_MAPPING, **PUBMED_RIS_KEYS}
    DEFAULT_DELIMITER_MAPPING = rispy.config.DELIMITED_TAG_MAPPING

    counter_re = re.compile("^[0-9]+.")

    def get_content(self, line: str) -> str:
        return line[6:].strip()

    def is_header(self, line: str) -> bool:
        none_or_match = self.counter_re.match(line)
        return bool(none_or_match)


def read_scopus_ris_file(filepath: str,
                         project_id: str | uuid.UUID | None = None) -> Generator[AcademicItemModel, None, None]:
    """
    This function will read a Scopus RIS file and return a generator of parsed `AcademicItemModel`s.

    :param filepath: path to the RIS file
    :param project_id: Optional, if set, will populate the returned items with this project_id
    :return:
    """

    with open(filepath, 'r') as fin:
        entries = ScopusParser().parse(text=fin.read())

        for entry in entries:

            scopus_id = None
            scopus_url: str | None = entry.get('scopus_url')
            if scopus_url is not None:
                matches = re.findall(r'\?eid=(.+?)&', scopus_url)
                if len(matches) > 0:
                    scopus_id = matches[0]

            meta = {
                k: _get(entry, k)
                for k in
                ['abbreviated_source_title', 'article_number', 'editors', 'issn', 'issue', 'language', 'first_page',
                 'last_page', 'conference_name', 'conference_date', 'conference_location', 'pmid', 'proceedings_title',
                 'publication_year', 'publisher', 'scopus_db', 'scopus_url', 'second_article_title',
                 'type_of_reference', 'document_type', 'conference_sponsors', 'volume']
            }
            authors = None
            s_authors = _get(entry, 'authors')
            if s_authors is not None and len(s_authors) > 0:
                authors = [
                    AcademicAuthorModel(name=au)
                    for au in s_authors
                ]

            doc = AcademicItemModel(project_id=project_id,
                                    scopus_id=scopus_id,
                                    doi=_get(entry, 'doi'),
                                    title=_get(entry, 'article_title'),
                                    text=_get(entry, 'abstract'),
                                    publication_year=(int(_get(entry, 'publication_year'))  # type: ignore[arg-type]
                                                      if _get(entry, 'publication_year') is not None else None),
                                    keywords=_get(entry, 'keywords'),  # type: ignore[arg-type]
                                    pubmed_id=_get(entry, 'pmid'),
                                    title_slug=str_to_title_slug(_get(entry, 'article_title')),
                                    source=_get(entry, 'abbreviated_source_title'),
                                    authors=authors,
                                    meta=clear_empty(meta))
            yield doc
