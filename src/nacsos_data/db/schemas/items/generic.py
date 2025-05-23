import uuid
from sqlalchemy import ForeignKey
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy_json import mutable_json_type

from .base import Item
from . import ItemType


class GenericItem(Item):
    __tablename__ = 'generic_item'
    item_id = mapped_column(UUID(as_uuid=True),
                            ForeignKey(Item.item_id, ondelete='CASCADE'),
                            default=uuid.uuid4, nullable=False, index=True, primary_key=True, unique=True)

    # any kind of (json-formatted) meta-data
    #   For project marked as "basic" this information may be shown to the user.
    #   Keys with prefix `_` will not be rendered by the frontend though.
    meta = mapped_column(mutable_json_type(dbtype=JSONB(none_as_null=True), nested=True))

    __mapper_args__ = {
        'polymorphic_identity': ItemType.generic,
    }
