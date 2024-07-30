import re
from ..models.items import AnyItemModel

TOKEN_PATTERN = re.compile(r'(?u)\b\w\w+\b')
REG_CLEAN = re.compile(r'[^a-z ]+', flags=re.IGNORECASE)


def tokenise_text(txt: str | None, lowercase: bool = False) -> list[str]:
    if txt is None:
        return []
    if lowercase:
        return TOKEN_PATTERN.findall(txt.lower())
    return TOKEN_PATTERN.findall(txt)


def tokenise_item(item: AnyItemModel, lowercase: bool = False) -> list[str]:
    return tokenise_text(item.text, lowercase=lowercase)


def extract_vocabulary(token_counts: dict[str, int], min_count: int = 1, max_features: int = 1000) -> list[str]:
    filtered_vocab = [(tok, cnt) for tok, cnt in token_counts.items() if cnt > min_count]
    vocab = [tok for tok, _ in sorted(filtered_vocab, key=lambda x: x[1], reverse=True)]
    return vocab[:max_features]


def clean_text(txt: str | None) -> str | None:
    if txt is None:
        return None
    return REG_CLEAN.sub(' ', txt.lower()).strip()
