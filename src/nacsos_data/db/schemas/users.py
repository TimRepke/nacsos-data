from typing import TYPE_CHECKING
from sqlalchemy import String, Boolean, ForeignKey, DateTime, func
from sqlalchemy.dialects.postgresql import UUID
import uuid

from sqlalchemy.orm import mapped_column, Relationship, relationship

from ..base_class import Base

if TYPE_CHECKING:
    from .projects import ProjectPermissions


class User(Base):
    """
    User represents a person.
    Most entries in the database will be (indirectly) linked to user accounts, so this is
    at the core of access management and ownership.
    """
    __tablename__ = 'user'

    # Unique identifier for this user.
    user_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                            nullable=False, unique=True, index=True)

    # Username for that user
    # -> nicer than using email and allows us to have multiple accounts per email
    username = mapped_column(String, nullable=False, unique=True, index=True)

    # Contact information for that user
    email = mapped_column(String, nullable=False, unique=True, index=True)

    # Real name of that user (or "descriptor" if this is a bot account)
    full_name = mapped_column(String, nullable=False)

    # Affiliation of the user, helpful to keep track of external users
    affiliation = mapped_column(String, nullable=True)

    # Hashed password
    # via CryptContext(schemes=["bcrypt"], deprecated="auto").hash(plaintext_password)
    password = mapped_column(String, nullable=True)

    # Set this flag if this account has root access to the database
    is_superuser = mapped_column(Boolean, nullable=False, default=False)

    # Set this flag to indicate whether the account is active or not
    # Note: Deleting an account might lead to inconsistencies with other parts of the DB,
    #       so setting this to "false" to remove access should be preferred.
    is_active = mapped_column(Boolean, nullable=False, default=True)

    # Date and time when this user was created (or last updated)
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    time_updated = mapped_column(DateTime(timezone=True), onupdate=func.now(), nullable=True)

    project_permissions: Relationship['ProjectPermissions'] = relationship('ProjectPermissions', cascade='all, delete')
    auth_tokens: Relationship['AuthToken'] = relationship('AuthToken', cascade='all, delete')


class AuthToken(Base):
    """
    Stores the JSON Web Tokens for user authentication.
    A user might have multiple tokens, e.g. in order to use as API tokens.
    """
    __tablename__ = 'auth_tokens'

    token_id = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                             nullable=False, unique=True, index=True)

    # Refers to the User this auth token belongs to
    username = mapped_column(String,
                             ForeignKey(User.username),
                             nullable=False, index=True, unique=False)

    # Date and time when this token was created (or last updated)
    time_created = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    time_updated = mapped_column(DateTime(timezone=True), onupdate=func.now(), nullable=True)

    # Timestamp to indicate until when this token is valid; null means valid forever
    valid_till = mapped_column(DateTime(timezone=True), nullable=True)
