import uuid
from sqlalchemy import String, Integer, ForeignKey, UniqueConstraint, Column
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import mapped_column, column_property, Mapped, Relationship, relationship
from sqlalchemy_json import mutable_json_type

from . import ItemType
from .. import Import
from .base import Item
from ..projects import Project
from ...base_class import Base


class AcademicItem(Item):
    __tablename__ = 'academic_item'
    __table_args__ = (
        UniqueConstraint('wos_id', 'project_id'),
        UniqueConstraint('scopus_id', 'project_id'),
        UniqueConstraint('s2_id', 'project_id'),
        UniqueConstraint('pubmed_id', 'project_id'),
        # UniqueConstraint('doi', 'project_id'),
        # UniqueConstraint('openalex_id', 'project_id'),
        # UniqueConstraint('title_slug', 'project_id'),
    )

    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(Item.item_id, ondelete='CASCADE'),
                            default=uuid.uuid4, nullable=False, index=True, primary_key=True, unique=True)

    # mirror of `Item.project_id` so we can introduce the UniqueConstraint
    # https://docs.sqlalchemy.org/en/20/faq/ormconfiguration.html#i-m-getting-a-warning-or-error-about-implicitly-combining-column-x-under-attribute-y
    project_id: Mapped[uuid.UUID] = column_property(Column(UUID(as_uuid=True),  # type: ignore[assignment]
                                                           ForeignKey(Project.project_id, ondelete='cascade'),
                                                           index=True, nullable=False), Item.project_id)

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
    keywords = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True), nullable=True, index=True)

    # JSON representation of authors: see models/academic.py
    authors = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True))

    # abstract inherited from `Item` as `Item.text`

    # any kind of (json-formatted) meta-data
    #   For project marked as "basic" this information may be shown to the user.
    #   Keys with prefix `_` will not be rendered by the frontend though.
    meta = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True))

    variants: Relationship['AcademicItemVariant'] = relationship('AcademicItemVariant',
                                                                 cascade='all, delete')

    __mapper_args__ = {
        'polymorphic_identity': ItemType.academic,
    }


class AcademicItemVariant(Base):
    """
    This Class/Table mostly mirrors `AcademicItem`, please refer to that definition for additional comments.
    The main purpose of this table is to be used in the context of keeping track of duplicates.

    In particular, when we insert something into the `AcademicItem` table, we first check for duplicates.
    If we find one, we insert something here
      1. if, for the item_id we found, there's nothing here, copy that row from `AcademicItem` and insert it here
      2. if at least one field does not equal what we had before, add the academic item here
      3. fuse the new and existing item and update it in the AcademicItem table

    Note, that we only keep unique values here.
    """
    __tablename__ = 'academic_item_variant'

    __table_args__ = (
        UniqueConstraint('item_id', 'doi'),
        UniqueConstraint('item_id', 'wos_id'),
        UniqueConstraint('item_id', 'scopus_id'),
        UniqueConstraint('item_id', 'openalex_id'),
        UniqueConstraint('item_id', 's2_id'),
        UniqueConstraint('item_id', 'pubmed_id'),
        UniqueConstraint('item_id', 'title'),
        UniqueConstraint('item_id', 'publication_year'),
        UniqueConstraint('item_id', 'source'),

        # No constraint, too complicated:
        # UniqueConstraint('item_id', 'meta')
        # UniqueConstraint('item_id', 'authors')
        # UniqueConstraint('item_id', 'keywords')
        # UniqueConstraint('item_id', 'abstract')
    )

    item_variant_id = mapped_column(UUID(as_uuid=True),
                                    primary_key=True, default=uuid.uuid4,
                                    nullable=False, unique=True, index=True)

    # Reference to the `AcademicItem` this is a duplicate of
    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(AcademicItem.item_id, ondelete='CASCADE'),
                            nullable=False, index=True, unique=False)

    # (Optional) reference to the import where this variant came from
    import_id = mapped_column(UUID(as_uuid=True),
                              ForeignKey(Import.import_id),
                              nullable=True, index=False, unique=False)

    doi = mapped_column(String, nullable=True, unique=False, index=False)
    wos_id = mapped_column(String, nullable=True, unique=False, index=False)
    scopus_id = mapped_column(String, nullable=True, unique=False, index=False)
    openalex_id = mapped_column(String, nullable=True, unique=False, index=False)
    s2_id = mapped_column(String, nullable=True, unique=False, index=False)
    pubmed_id = mapped_column(String, nullable=True, unique=False, index=False)
    title = mapped_column(String, nullable=True, unique=False, index=False)
    publication_year = mapped_column(Integer, nullable=True, unique=False, index=False)
    source = mapped_column(String, nullable=True, unique=False, index=False)
    keywords = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True), nullable=True,
                             index=False)
    authors = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True), nullable=True, index=False)
    abstract = mapped_column(String, nullable=True, unique=False, index=False)
    meta = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True), nullable=True, index=False)
