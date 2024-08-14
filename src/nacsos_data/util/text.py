import re
from ..models.items import AnyItemModel

TOKEN_PATTERN = re.compile(r'(?u)\b\w\w+\b')
REG_CLEAN = re.compile(r'[^a-z ]+', flags=re.IGNORECASE)
CLEAN_HTML = re.compile('<[a-zA-Z/]+[^>]*>')


def preprocess_text(x: str | None) -> str:
    '''
    Preprocesses text by removing html tags (like <sub> <sup>) and lowering the case
    :param x: a string to be preprocessed
    :return: preprocessed string
    '''
    return re.sub(CLEAN_HTML, '', str(x)).lower()


def tokenise_text(txt: str | None, lowercase: bool = True, max_tokens: int = 80) -> list[str]:
    '''
    :param txt: a text string to be tokenized
    :param lowercase: lowercase or not, no reasons why not normally
    :param max_tokens: only return the first max_tokens tokens (to deal with truncated abstracts)
    :return: a list of tokens
    '''
    if txt is None:
        return []
    if lowercase:
        return TOKEN_PATTERN.findall(txt.lower())[:max_tokens]
    return TOKEN_PATTERN.findall(txt)[:max_tokens]


def tokenise_item(item: AnyItemModel, lowercase: bool = True) -> list[str]:
    return tokenise_text(preprocess_text(item.text), lowercase=lowercase)


def extract_vocabulary(token_counts: dict[str, int], min_count: int = 1, max_features: int = 1000) -> list[str]:
    filtered_vocab = [(tok, cnt) for tok, cnt in token_counts.items() if cnt > min_count]
    vocab = [tok for tok, _ in sorted(filtered_vocab, key=lambda x: x[1], reverse=True)]
    return vocab[:max_features]


def clean_text(txt: str | None) -> str | None:
    if txt is None:
        return None
    return REG_CLEAN.sub(' ', txt.lower()).strip()
