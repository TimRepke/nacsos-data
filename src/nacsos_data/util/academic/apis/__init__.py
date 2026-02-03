from enum import Enum
from typing import Type

from .dimensions import DimensionsAPI
from .openalex import OpenAlexAPI, OpenAlexSolrAPI
from .pubmed import PubmedAPI
from .util import AbstractAPI
from .wos import WoSAPI
from .scopus import ScopusAPI


class APIEnum(str, Enum):
    SOLR = 'SOLR'
    OA = 'OA'
    WOS = 'WOS'
    PUBMED = 'PUBMED'
    DIMENSIONS = 'DIMENSIONS'
    SCOPUS = 'SCOPUS'


APIMap: dict[APIEnum, Type[AbstractAPI]] = {
    APIEnum.SOLR: OpenAlexSolrAPI,
    APIEnum.OA: OpenAlexAPI,
    APIEnum.WOS: WoSAPI,
    APIEnum.DIMENSIONS: DimensionsAPI,
    APIEnum.PUBMED: PubmedAPI,
    APIEnum.SCOPUS: ScopusAPI,
}

__all__ = [
    'DimensionsAPI',
    'ScopusAPI',
    'PubmedAPI',
    'OpenAlexAPI',
    'OpenAlexSolrAPI',
    'WoSAPI',
    'APIEnum',
    'APIMap',
]
