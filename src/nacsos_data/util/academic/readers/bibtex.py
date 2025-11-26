import uuid
import logging
from pathlib import Path
from typing import Generator
import bibtexparser
from bibtexparser.middlewares import NameParts, BlockMiddleware
from bibtexparser.model import Entry, Block
from bibtexparser import middlewares, Library

from ....models.items import AcademicItemModel
from ....models.items.academic import AcademicAuthorModel
from ..clean import clear_empty
from ..duplicate import str_to_title_slug

logger = logging.getLogger('nacsos_data.util.academic.bibtex')


class EnsureUniqKeyMiddleware(BlockMiddleware):  # type: ignore[misc]
    """Sorts the fields of an entry alphabetically by key."""

    def __init__(self, allow_inplace_modification: bool = True):
        super().__init__(
            allow_inplace_modification=allow_inplace_modification,
            allow_parallel_execution=True,
        )
        self.cnt = 0

    # docstr-coverage: inherited
    def transform_entry(self, entry: Entry, library: Library) -> Block:
        entry.parser_metadata[self.metadata_key()] = True
        entry.key = f"{entry.key} | {self.cnt}"
        self.cnt += 1
        return entry

    # docstr-coverage: inherited
    @classmethod
    def metadata_key(cls) -> str:
        return "force_unique_key"


def _ensure_http_doi(doi: str | None) -> str | None:
    if doi is None or len(doi.strip()) == 0:
        return None
    if doi.startswith('http'):
        return doi
    return f'https://doi.org/{doi.strip()}'


def _translate_authors(authors: list[NameParts] | None) -> list[AcademicAuthorModel] | None:
    if authors is None:
        return None

    def merge(p: list[str]) -> str:
        return ' '.join(p)

    return [AcademicAuthorModel(name=f'{merge(author.first)}{merge(author.von)} {merge(author.last)}',
                                surname_initials=f'{merge(author.last)}{", " + author.first[0][0] if len(author.first) > 0 else ""}')
            for author in authors]


def _tanslate_yr(yr: str | None) -> int | None:
    if yr is None or len(yr.strip()) != 4:
        return None
    return int(yr.strip())


def _translate_kw(kws: str | None) -> list[str] | None:
    if kws is None or len(kws) == 0:
        return None
    return [kw.strip() for kw in kws.split(',')]


def generate_entries_from_bibtex(file: Path,
                                 log: logging.Logger | None = None) -> Generator[Entry, None, None]:
    if log is None:
        log = logger

    log.info(f'Parsing BibTeX file: {file}')
    library = bibtexparser.parse_file(str(file), append_middleware=[
        EnsureUniqKeyMiddleware(),
        middlewares.SeparateCoAuthors(),
        middlewares.SplitNameParts()
    ])

    yield from library.entries


def generate_items_from_bibtex(file: Path,
                               project_id: str | uuid.UUID | None = None,
                               log: logging.Logger | None = None) -> Generator[AcademicItemModel, None, None]:
    """
    Example script on how to use this for importing data to the platform

    ```python
        import asyncio
        import logging
        from pathlib import Path

        from nacsos_data.db import get_engine_async
        from nacsos_data.util.academic.bibtex import generate_items_from_bibtex
        from nacsos_data.util.academic.importer import import_academic_items

        logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=logging.INFO)
        logger = logging.getLogger('import')
        logger.setLevel(logging.DEBUG)

        db_engine = get_engine_async(conf_file='/path/to/server.env')

        PROJECT_ID = '???'
        USER_ID = '???'  # ID of user used for the import
        IMPORT_NAME = '??'  # DO NOT SKIP THIS! YOU WILL FORGET WHAT THAT WAS SOONER THAN YOU THINK
        IMPORT_DESC = 'Description for this import'  # DO NOT SKIP THIS! YOU WILL FORGET WHAT THAT WAS SOONER THAN YOU THINK
        SOURCE_FILE = '/path/to/references.bib'


        def items():
            yield from generate_items_from_bibtex(Path(SOURCE_FILE))


        async def main():
            async with db_engine.session() as session:
                await import_academic_items(
                    session=session,
                    project_id=PROJECT_ID,
                    new_items=items,
                    import_name=IMPORT_NAME,
                    user_id=USER_ID,
                    description=IMPORT_DESC,
                    batch_size=2500,
                    dry_run=False,
                    trust_new_authors=True,
                    trust_new_keywords=True,
                    log=logger)


        if __name__ == '__main__':
            asyncio.run(main())
    ```

    :param file:
    :param project_id:
    :param log:
    :return:
    """
    if log is None:
        log = logger

    for entry in generate_entries_from_bibtex(file=file, log=log):
        d = dict(entry.items())
        yield AcademicItemModel(
            item_id=None,
            project_id=project_id,
            doi=_ensure_http_doi(d.get('doi')),
            title=d.get('title'),
            title_slug=str_to_title_slug(d.get('title')),
            text=d.get('abstract'),
            publication_year=_tanslate_yr(d.get('year')),
            source=d.get('journal', d.get('booktitle')),
            keywords=_translate_kw(d.get('keywords')),
            authors=_translate_authors(d.get('author')),
            meta=clear_empty({
                **d,
                'author': [a.__dict__ for a in d.get('author', [])],
                'editor': [e.__dict__ for e in d.get('editor', [])]
            })
        )


def ensure_non_duplicate_keys(infile: Path, outfile: Path) -> None:
    """
    Sometimes, there are duplicate or empty keys in the bibtex file.
    The library we use for parsing doesn't handle that well.
    As a workaround for now, use this method, which just appends a counter at the end of all keys.
    Not elegant, but gets the job done.

    :param infile:
    :param outfile:
    :return:
    """
    from tqdm import tqdm
    import re
    p = re.compile(r'(@\w+){([^,]*),')
    cnt = 0
    with (open(infile, 'r') as fin,
          open(outfile, 'w') as fout):
        for line in tqdm(fin):
            line = p.sub(f'\\1{{\\2-{cnt},', line)
            cnt += 1
            fout.write(f'{line}')
