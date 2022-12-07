from sqlalchemy import String, Integer, DateTime, Float, ForeignKey, UniqueConstraint, Column
from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy_json import mutable_json_type
from sqlalchemy.dialects.postgresql import UUID, JSONB

from .base import Item
from . import ItemType


# TODO define schema
# TODO mirror in model
# TODO add to schemas.__all__


class AcademicItem(Item):
    __tablename__ = 'academic_item'
    item_id = mapped_column(UUID(as_uuid=True), ForeignKey(Item.item_id), primary_key=True)

    doi = mapped_column(String, nullable=True, unique=False, index=True)

    ## Summarise design decisions in the documentation

    # Set unique constraints on proprietary IDs and project
    wos_id = mapped_column(String, nullable=True, unique=False, index=True)
    scopus_id = mapped_column(String, nullable=True, unique=False, index=True)
    openalex_id = mapped_column(String, nullable=True, unique=False, index=True)
    s2_id = mapped_column(String, nullable=True, unique=False, index=True)

    # (Primary) title of the paper
    title = mapped_column(String, nullable=True, unique=False, index=True)
    # lower case string of title
    title_slug = mapped_column(String, nullable=True, unique=False, index=True)

    publication_year = mapped_column(Integer, nullable=True, unique=False, index=True)

    # Journal
    source = mapped_column(String, nullable=True, unique=False, index=True)

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
