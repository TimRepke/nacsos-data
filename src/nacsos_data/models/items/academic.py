import uuid
from typing import Any

from .base import ItemModel
from uuid import UUID
from pydantic import BaseModel

from nacsos_data.db.schemas import ItemType


class AffiliationModel(BaseModel):
    name: str
    country: str | None = None
    openalex_id: str | None = None  # OpenAlex ID (if known/present)
    s2_id: str | None = None  # SemanticScholar ID (if known/present)


class AcademicAuthorModel(BaseModel):
    # Name is given by AF in web of science, and is in the format
    # First name, other names. These are sometimes shortened to
    # initials, depending on the information provided to the publisher
    name: str
    # Surname initials is given in the format Surname, AB - where
    # AB are the initials of the first names
    surname_initials: str | None = None
    email: str | None = None
    orcid: str | None = None  # ORCID (if known/present)
    scopus_id: str | None = None  # Scopus ID (if known/present)
    openalex_id: str | None = None  # OpenAlex ID (if known/present)
    s2_id: str | None = None  # SemanticScholar ID (if known/present)
    dimensions_id: str | None = None  # Dimensions ID (if known/present)

    affiliations: list[AffiliationModel] | None = None

    meta: dict[str, Any] | None = None


class AcademicItemModel(ItemModel):
    """
    Corresponds to db.schema.items.academic.AcademicItem
    """

    item_id: str | UUID | None = None
    doi: str | None = None
    type: ItemType = ItemType.academic

    wos_id: str | None = None
    scopus_id: str | None = None
    openalex_id: str | None = None
    s2_id: str | None = None
    pubmed_id: str | None = None
    dimensions_id: str | None = None

    # (Primary) title of the paper
    title: str | None = None
    title_slug: str | None = None

    publication_year: int | None = None

    # Journal
    source: str | None = None

    keywords: list[str] | None = None

    authors: list[AcademicAuthorModel] | None = None

    meta: dict[str, Any] | None = None


class AcademicItemVariantModel(BaseModel):
    item_variant_id: str | uuid.UUID
    item_id: str | uuid.UUID
    import_id: str | uuid.UUID | None = None
    import_revision: int | None = None
    doi: str | None = None
    wos_id: str | None = None
    scopus_id: str | None = None
    openalex_id: str | None = None
    s2_id: str | None = None
    pubmed_id: str | None = None
    dimensions_id: str | None = None
    title: str | None = None
    publication_year: int | None = None
    source: str | None = None
    keywords: list[str] | None = None
    authors: list[AcademicAuthorModel] | None = None
    text: str | None = None
    meta: dict[str, Any] | None = None
