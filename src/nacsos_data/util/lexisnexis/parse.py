import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Generator
from xml.etree import ElementTree

from nacsos_data.db.schemas import ItemType
from nacsos_data.models.items.lexis_nexis import (
    NewsSearchResult,
    LexisNexisItemSourceModel,
    LexisNexisItemModel,
    LexisNexisDocument
)
from nacsos_data.util import clear_empty

logger = logging.getLogger('nacsos_data.util.LexisNexis.parse')

prefix_map = {
    # '': 'http://www.w3.org/2005/Atom',
    'atom': 'http://www.w3.org/2005/Atom',
    'nitf': 'http://iptc.org/std/NITF/2006-10-18',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'noNamespaceSchemaLocation': 'http://www.lexisnexis.com/xmlschemas/content/public/articledoc/1/'
}


def find_text(root: ElementTree.Element, xpath: str) -> str | None:
    elem = root.find(xpath, namespaces=prefix_map)
    if elem is not None:
        return elem.text
    return None


def find_texts(root: ElementTree.Element, xpath: str) -> list[str] | None:
    elem = root.find(xpath, namespaces=prefix_map)
    if elem is not None:
        return list(elem.itertext())
    return None


def joined_texts(root: ElementTree.Element, xpath: str, join: str = ' ') -> str | None:
    texts = find_texts(root=root, xpath=xpath)
    if texts is not None:
        return join.join(texts)
    return None


def lst_texts(root: ElementTree.Element, xpath: str) -> list[str] | None:
    elems = root.findall('./atom:content/articleDoc/articleDocHead//author', namespaces=prefix_map)
    if elems and len(elems) > 0:
        return [' '.join(e.itertext()).strip() for e in elems]
    return None


def parse_document(document: str) -> LexisNexisDocument:
    root = ElementTree.fromstring(document)

    return LexisNexisDocument(
        title=find_text(root, './/{http://www.w3.org/2005/Atom}title'),
        published=find_text(root, './/{http://www.w3.org/2005/Atom}published'),
        updated=find_text(root, './/{http://www.w3.org/2005/Atom}updated'),
        # (Alternative) title and teaser text
        teaser=joined_texts(root, './atom:content/articleDoc'
                                  '/{http://iptc.org/std/NITF/2006-10-18/}body'
                                  '/{http://iptc.org/std/NITF/2006-10-18/}body.head', '\n'),

        # Actual content
        text=joined_texts(root, './atom:content/articleDoc'
                                '/{http://iptc.org/std/NITF/2006-10-18/}body'
                                '/{http://iptc.org/std/NITF/2006-10-18/}body.content', '\n'),
        authors=[' '.join(a.itertext()).strip()
                 for a in root.findall('./atom:content/articleDoc/articleDocHead//author', namespaces=prefix_map)],
        authors_sec=[' '.join(a.itertext()).strip()
                     for a in root.findall('./atom:author/atom:name', namespaces=prefix_map)],
        section=find_text(root, './atom:content/articleDoc/articleDocHead'
                                '/itemInfo/sourceSectionInfo/positionSection'),
        subsection=find_text(root, './atom:content/articleDoc/articleDocHead'
                                   '/itemInfo/sourceSectionInfo/positionSubsection')
    )


def s2dt_a(s: str | None) -> datetime | None:
    if s is not None:
        try:
            return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            pass
    return None


def s2dt_b(s: str | datetime | None) -> datetime | None:
    if s is not None:
        if isinstance(s, datetime):
            return s
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S%z')
        except ValueError:
            pass
    return None


def translate_search_result(result: NewsSearchResult, project_id: str | None = None) \
        -> tuple[LexisNexisItemModel, LexisNexisItemSourceModel]:
    if result.Document is None or result.Document.Content is None:
        raise ValueError('Missing document content')

    ln_id = result.ResultId
    if ln_id is None:
        raise ValueError('Missing LexisNexis ID')

    doc = parse_document(result.Document.Content)

    item = LexisNexisItemModel(
        project_id=project_id,
        item_id=str(uuid.uuid4()),
        text=doc.text,
        type=ItemType.lexis,
        teaser=doc.teaser,
        authors=doc.authors or doc.authors_sec,
    )

    sections = result.Section
    if doc.section or doc.subsection:
        sections = f'{doc.section} | {doc.subsection}'

    src = LexisNexisItemSourceModel(
        item_source_id=str(uuid.uuid4()),
        item_id=item.item_id,
        lexis_id=ln_id,
        name=(result.Source.Name if result.Source else None) or result.Overview,
        title=doc.title or result.Title,
        section=sections,
        jurisdiction=result.Jurisdiction,
        location=result.Location,
        content_type=result.ContentType,
        published_at=s2dt_a(doc.published) or s2dt_b(result.Date),
        updated_at=s2dt_a(doc.updated),
        meta=clear_empty({
            'Date': result.Date,
            'published': doc.published,
            'updated': doc.updated,
            'authors_1': doc.authors,
            'authors_2': doc.authors_sec,
            'section': doc.section,
            'subsection': doc.subsection,
            'Jurisdiction': result.Jurisdiction,
            'Location': result.Location,
            'ContentType': result.ContentType,
            'Byline': result.Byline,
            'WordLength': result.WordLength,
            'WebNewsUrl': result.WebNewsUrl,
            'Geography': result.Geography,
            'Language': result.Language,
            'Industry': result.Industry,
            'People': result.People,
            'Subject': result.Subject,
            'SectionRes': result.Section,
            'Company': result.Company,
            'PublicationType': result.PublicationType,
            'Publisher': result.Publisher,
            'LEI': result.LEI,
            'CompanyName': result.CompanyName,
            'LNGI': result.LNGI,
            'MediaLink': result.DocumentContent_odata_mediaReadLink,
            'MediaType': result.DocumentContent_odata_mediaContentType,
            'Overview': result.Overview,
            'TitleRes': result.Title,
            'TitleDoc': doc.title,
            'Topic': result.Topic,
            'PracticeArea': result.PracticeArea
        })
    )

    return item, src


def parse_lexis_nexis_file(filename: Path | str, project_id: str | None = None, fail_on_error: bool = True) \
        -> Generator[tuple[NewsSearchResult, LexisNexisItemModel, LexisNexisItemSourceModel], None, None]:
    file_path = Path(filename)
    if file_path.exists():
        with open(file_path, 'r') as f_src:
            for li, line in enumerate(f_src):
                try:
                    result = NewsSearchResult.model_validate_json(line)
                    item, src = translate_search_result(result, project_id=project_id)
                    yield result, item, src
                except Exception as e:
                    logger.warning(f'Problem in line {li + 1}')
                    logger.exception(e)
                    if fail_on_error:
                        raise e