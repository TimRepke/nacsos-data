from typing import Optional
from pydantic import BaseModel, EmailStr


# Shared properties
class UserBaseModel(BaseModel):
    """
    User represents a person.
    Most entries in the database will be (indirectly) linked to user accounts, so this is
    at the core of access management and ownership.
    """
    # Unique identifier for this user.
    user_id: Optional[str]

    # Username for that user
    # -> nicer than using email and allows us to have multiple accounts per email
    username: Optional[str] = None

    # Contact information for that user
    email: Optional[EmailStr] = None

    # Real name of that user (or "descriptor" if this is a bot account)
    full_name: Optional[str] = None

    # Affiliation of the user, helpful to keep track of external users
    affiliation: Optional[str] = None

    # Set this flag if this account has root access to the database
    is_superuser: Optional[bool] = False

    # Set this flag to indicate whether the account is active or not
    # Note: Deleting an account might lead to inconsistencies with other parts of the DB,
    #       so setting this to "false" to remove access should be preferred.
    is_active: Optional[bool] = True


# Properties to receive via API on creation
class UserCreateModel(UserBaseModel):
    email: EmailStr
    username: str
    plain_password: str


# Properties to receive via API on update
class UserUpdateModel(UserBaseModel):
    plain_password: Optional[str] = None


class UserInDBBaseModel(UserBaseModel):
    id: Optional[str] = None

    # class Config:
    #     orm_mode = True


# Additional properties to return via API
class UserModel(UserInDBBaseModel):
    pass


# Additional properties stored in DB
class UserInDBModel(UserInDBBaseModel):
    # Hashed password
    # via CryptContext(schemes=["bcrypt"], deprecated="auto").hash(plaintext_password)
    password: str
