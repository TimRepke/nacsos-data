import re
from typing import Generator
from ...models.items import AcademicItemModel
from .wosfile.record import records_from
from ...models.items.academic import AcademicAuthorModel, AffiliationModel

REGEX_C1 = re.compile(r'\[([^\]]+)\] (.*), (.*).')


def read_wos_file(filepath: str) -> Generator[AcademicItemModel, None, None]:
    for record in records_from([filepath]):
        item = AcademicItemModel()

        if record.get('TI') and len(record.get('TI')) > 0:
            item.title = record.get('TI')  # type: ignore[assignment]
            # title_slug will be added on insert

        if record.get('DI') and len(record.get('DI')) > 0:
            item.doi = record.get('DI')  # type: ignore[assignment]

        if record.get('UT') and len(record.get('UT')) > 0:
            item.wos_id = record.get('UT')  # type: ignore[assignment]

        # FIXME: add following lines once pubmed is part of model
        # if record.get('PM') and len(record.get('PM')) > 0:
        #    item.pubmed_id = record.get('PM')  # type: ignore[assignment]

        if record.get('PY') and len(record.get('PY')) > 0:
            item.publication_year = int(record.get('PY'))  # type: ignore[assignment]

        if record.get('AB') and len(record.get('AB')) > 0:
            item.text = record.get('AB')  # type: ignore[assignment]

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
            except (ValueError, AttributeError) as e:
                pass
        for affiliation_entry in record.get('C1', []):
            try:
                authors_str, institute, country = REGEX_C1.findall(affiliation_entry)
                authors_lst = authors_str.split('; ')
                for name in authors_lst:
                    if name in authors:
                        if authors[name].affiliations is None:
                            authors[name].affiliations = []
                        authors[name].affiliations.append(AffiliationModel(
                            name=institute,
                            country=country
                        ))
            except (ValueError, AttributeError) as e:
                pass
        item.authors = list(authors.values())

        item.meta = {
            'wos': dict(record)
        }

        yield item
