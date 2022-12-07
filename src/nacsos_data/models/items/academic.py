from .base import ItemModel
from uuid import UUID
from pydantic import BaseModel


class AffiliationModel(BaseModel):
    name: str
    country: str | None = None
    openalex_id: str | None = None
    s2_id: str | None = None


class AcademicAuthorModel(BaseModel):
    name: str
    surname_initials: str | None = None
    orcid: str | None = None
    affiliations: list[AffiliationModel] | None = None

class AcademicItemModel(ItemModel):
    """
    Corresponds to db.schema.items.academic.AcademicItem
    """
    item_id: str | UUID | None = None
    doi: str | None = None

    wos_id: str | None = None
    scopus_id: str | None = None
    openalex_id: str | None = None
    s2_id: str | None = None

    # (Primary) title of the paper
    title: str | None = None
    title_slug: str | None = None

    publication_year: int | None = None

    # Journal
    source: str | None = None

    keywords: list[str] | None = None

    authors: list[AcademicAuthorModel] | None = None
    pass
