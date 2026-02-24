import re
from datetime import date

from nacsos_data.db.crud.items.academic import IdField
from nacsos_data.models.items import AcademicItemModel

YEAR_PATTERN = re.compile(r'\d{4}')
REGEX_NON_ALPH = re.compile(r'[^a-z]')
REGEX_NON_ALPHNUM = re.compile(r'[^a-z0-9]')
LATEST_POSSIBLE_PUB_YEAR = date.today().year + 5
# determined by case study at https://gitlab.pik-potsdam.de/mcc-apsis/nacsos/case-studies/duplicate-detection/-/blob/main/202407_experiments/avg_abstract_len.sql?ref_type=heads
MAX_ABSTRACT_LENGTH = 12000
MAX_TITLE_LENGTH = 1000
MIN_TSLUG_LEN = 20
MAX_TSLUG_LEN = 500
ID_FIELDS: list[IdField] = ['openalex_id', 's2_id', 'scopus_id', 'wos_id', 'pubmed_id', 'dimensions_id']


def str_to_title_slug(title: str | None) -> str | None:
    if title is None or len(title.strip()) == 0:
        return None
    # remove all non-alphabetic characters
    return REGEX_NON_ALPH.sub('', title.lower())[:MAX_TSLUG_LEN]


def get_title_slug(item: AcademicItemModel) -> str | None:
    return str_to_title_slug(item.title)
