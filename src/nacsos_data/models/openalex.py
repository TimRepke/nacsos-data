from typing import Literal

from pydantic import BaseModel

DefType = Literal['edismax', 'lucene', 'dismax']
SearchField = Literal['title', 'abstract', 'title_abstract']
OpType = Literal['OR', 'AND']


class Biblio(BaseModel):
    volume: str | None = None
    issue: str | None = None
    first_page: str | None = None
    last_page: str | None = None


class DehydratedInstitution(BaseModel):
    country_code: str | None = None
    display_name: str | None = None
    id: str | None = None
    ror: str | None = None
    type: str | None = None


class DehydratedAuthor(BaseModel):
    display_name: str | None = None
    id: str | None = None
    orcid: str | None = None


class Authorship(BaseModel):
    author: DehydratedAuthor | None = None
    author_position: str | None = None
    institutions: list[DehydratedInstitution] | None = None
    is_corresponding: bool | None = None
    raw_affiliation_string: str | None = None


class DehydratedSource(BaseModel):
    display_name: str | None = None
    host_organization: str | None = None
    # host_organization_lineage
    host_organization_name: str | None = None
    id: str | None = None
    # is_in_doaj
    # is_oa
    issn: list[str] | None = None
    issn_l: str | None = None
    type: str | None = None


class Location(BaseModel):
    is_oa: bool | None = None
    is_primary: bool | None = None
    landing_page_url: str | None = None
    license: str | None = None
    source: DehydratedSource | None = None
    pdf_url: str | None = None
    version: str | None = None


class _Work(BaseModel):
    id: str
    display_name: str | None = None
    title: str | None = None
    abstract: str | None = None
    title_abstract: str | None = None

    cited_by_count: int | None = None
    created_date: str | None = None
    doi: str | None = None
    mag: str | None = None
    pmid: str | None = None
    pmcid: str | None = None

    is_oa: bool | None = None
    is_paratext: bool | None = None
    is_retracted: bool | None = None
    language: str | None = None

    publication_date: str | None = None
    publication_year: int | None = None
    type: str | None = None
    updated_date: str | None = None


class WorkSolr(_Work):
    locations: str | None = None
    authorships: str | None = None
    biblio: str | None = None


class Work(_Work):
    locations: list[Location] | None = None
    authorships: list[Authorship] | None = None
    biblio: Biblio | None = None
