from typing import Annotated

from pydantic import Field

from .jsonl import TwitterDBFileImport, TwitterAPIFileImport
from .scopus import ScopusCSVImport
from .twitter import ImportConfigTwitter as TwitterImport
from .wos import WOSImport

ImportConfig = Annotated[TwitterDBFileImport
                         | TwitterAPIFileImport
                         | TwitterImport
                         | WOSImport
                         | ScopusCSVImport, Field(discriminator='func_name')]

APIParameters = ImportConfig

__all__ = ['TwitterImport', 'TwitterAPIFileImport', 'TwitterDBFileImport',
           'ScopusCSVImport', 'WOSImport',
           'ImportConfig', 'APIParameters']
