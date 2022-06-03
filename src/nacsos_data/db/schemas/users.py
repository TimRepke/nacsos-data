from sqlalchemy import String, Boolean, Column
from sqlalchemy.dialects.postgresql import UUID
import uuid

from ..base_class import Base


class User(Base):
    """
    User represents a person.
    Most entries in the database will be (indirectly) linked to user accounts, so this is
    at the core of access management and ownership.
    """
    __tablename__ = 'user'

    # Unique identifier for this user.
    user_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
                     nullable=False, unique=True, index=True)

    # Username for that user
    # -> nicer than using email and allows us to have multiple accounts per email
    username = Column(String, nullable=False, unique=True)

    # Contact information for that user
    email = Column(String, nullable=False, unique=True)

    # Real name of that user (or "descriptor" if this is a bot account)
    full_name = Column(String, nullable=False)

    # Affiliation of the user, helpful to keep track of external users
    affiliation = Column(String, nullable=True)

    # Hashed password
    # via CryptContext(schemes=["bcrypt"], deprecated="auto").hash(plaintext_password)
    password = Column(String, nullable=False)

    # Set this flag if this account has root access to the database
    is_superuser = Column(Boolean, nullable=False, default=False)

    # Set this flag to indicate whether the account is active or not
    # Note: Deleting an account might lead to inconsistencies with other parts of the DB,
    #       so setting this to "false" to remove access should be preferred.
    is_active = Column(Boolean, nullable=False, default=True)




