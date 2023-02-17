import uuid
from sqlalchemy import String, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import mapped_column
from sqlalchemy_json import mutable_json_type

from .base import Item
from . import ItemType


class AcademicItem(Item):
    __tablename__ = 'academic_item'
    __table_args__ = (
        UniqueConstraint('doi', 'project_id'),
        UniqueConstraint('wos_id', 'project_id'),
        UniqueConstraint('scopus_id', 'project_id'),
        UniqueConstraint('openalex_id', 'project_id'),
        UniqueConstraint('s2_id', 'project_id'),
        UniqueConstraint('pubmed_id', 'project_id'),
        UniqueConstraint('title_slug', 'project_id'),
    )

    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(Item.item_id, ondelete='CASCADE'),
                            default=uuid.uuid4, nullable=False, index=True, primary_key=True, unique=True)

    # Article DOI (normalised format, e.g. '00.000/0000.0000-00' rather than 'https://dx.doi.org/00.000/0000.0000-00')
    doi = mapped_column(String, nullable=True, unique=False, index=True)

    # wos ID exactly as it comes from WoS, including redudant WOS:
    wos_id = mapped_column(String, nullable=True, unique=False, index=True)
    scopus_id = mapped_column(String, nullable=True, unique=False, index=True)
    openalex_id = mapped_column(String, nullable=True, unique=False, index=True)
    s2_id = mapped_column(String, nullable=True, unique=False, index=True)
    pubmed_id = mapped_column(String, nullable=True, unique=False, index=True)

    # (Primary) title of the paper
    title = mapped_column(String, nullable=True, unique=False, index=False)
    # lower case string of title
    title_slug = mapped_column(String, nullable=True, unique=False, index=True)

    publication_year = mapped_column(Integer, nullable=True, unique=False, index=True)

    # Journal
    source = mapped_column(String, nullable=True, unique=False, index=True)

    # These should be the keywords given by the authors (in WoS author-keywords)
    # This should be a list of strings
    keywords = mapped_column(mutable_json_type(dbtype=JSONB, nested=True), nullable=True, index=True)

    # JSON representation of authors: see models/academic.py
    authors = mapped_column(mutable_json_type(dbtype=JSONB, nested=True))

    # abstract inherited from `Item` as `Item.text`

    # any kind of (json-formatted) meta-data
    #   For project marked as "basic" this information may be shown to the user.
    #   Keys with prefix `_` will not be rendered by the frontend though.
    meta = mapped_column(mutable_json_type(dbtype=JSONB, nested=True))

    __mapper_args__ = {
        'polymorphic_identity': ItemType.academic,
    }
