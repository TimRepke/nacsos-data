from typing import Annotated

from pydantic import Field

from .jsonl import TwitterDBFileImport, TwitterAPIFileImport, OpenAlexItemImport, AcademicItemImport
from .scopus import ScopusCSVImport
from .twitter import ImportConfigTwitter as TwitterImport
from .wos import WOSImport
from .openalex import OpenAlexImport

ImportConfig = Annotated[TwitterDBFileImport
                         | TwitterAPIFileImport
                         | TwitterImport
                         | OpenAlexImport
                         | WOSImport
                         | OpenAlexItemImport
                         | AcademicItemImport
                         | ScopusCSVImport, Field(discriminator='func_name')]

APIParameters = ImportConfig

__all__ = ['TwitterImport', 'TwitterAPIFileImport', 'TwitterDBFileImport',
           'ScopusCSVImport', 'WOSImport', 'OpenAlexImport', 'OpenAlexItemImport', 'AcademicItemImport',
           'ImportConfig', 'APIParameters']
