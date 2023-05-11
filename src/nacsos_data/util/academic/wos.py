import re
from typing import Generator
from ...models.items import AcademicItemModel
from .wosfile.record import records_from
from ...models.items.academic import AcademicAuthorModel, AffiliationModel

REGEX_C1 = re.compile(r'\[([^\]]+)\] (.*), (.*).')


def read_wos_file(filepath: str) -> Generator[AcademicItemModel, None, None]:
    for record in records_from([filepath]):
        item = AcademicItemModel()

        title = record.get('TI')
        if title and len(title) > 0:
            item.title = title  # type: ignore[assignment]
            # title_slug will be added on insert

        doi = record.get('DI')
        if doi and len(doi) > 0:
            item.doi = doi  # type: ignore[assignment]
        wos_id = record.get('UT')
        if wos_id and len(wos_id) > 0:
            item.wos_id = wos_id  # type: ignore[assignment]

        if record.get('PM') and len(record.get('PM')) > 0:  # type: ignore[arg-type]
            item.pubmed_id = record.get('PM')  # type: ignore[assignment]

        pub_year = record.get('PY')
        if pub_year and type(pub_year) == str and len(pub_year) > 0:
            item.publication_year = int(pub_year)  # type: ignore[assignment]

        abstract = record.get('AB')
        if abstract and len(abstract) > 0:
            item.text = abstract  # type: ignore[assignment]

        # There are several fields that could qualify as the "source".
        # Check them all and pick the first one that is valid (non-empty)
        source_candidates = [sc for sc in [record.get('JI'),
                                           record.get('SE'),
                                           record.get('SO'),
                                           record.get('CT'),
                                           record.get('J9')]
                             if sc is not None and len(sc) > 0]
        if len(source_candidates) > 0:
            item.source = source_candidates[0]  # type: ignore[assignment]

        keywords = [sci for sc in [record.get('ID', []), record.get('DE', [])] for sci in sc]
        if len(keywords) > 0:
            item.keywords = keywords  # type: ignore[assignment]

        authors = {
            author: AcademicAuthorModel(name=author)
            for author in record.get('AF', [])
        }
        for orcid_entry in record.get('OI', []):
            try:
                name, orcid = orcid_entry.split('/')
                if name in authors:
                    authors[name].orcid = orcid
            except (ValueError, AttributeError):
                pass
        for affiliation_entry in record.get('C1', []):
            try:
                authors_str, institute, country = REGEX_C1.findall(affiliation_entry)
                authors_lst = authors_str.split('; ')
                for name in authors_lst:
                    if name in authors:
                        author = authors[name]
                        if author.affiliations is None:
                            author.affiliations = []
                        author.affiliations.append(AffiliationModel(
                            name=institute,
                            country=country
                        ))
            except (ValueError, AttributeError):
                pass
        item.authors = list(authors.values())

        item.meta = {
            'wos': dict(record)
        }

        yield item
