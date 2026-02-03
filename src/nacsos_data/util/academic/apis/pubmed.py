import uuid
from typing import Any, Generator
from xml.etree.ElementTree import Element, fromstring as parse_xml

from httpx import codes, Response

from nacsos_data.util import as_uuid, clear_empty
from nacsos_data.util.xml import xml2dict
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI
from nacsos_data.models.items.academic import AcademicAuthorModel, AcademicItemModel, AffiliationModel


def select(obj: dict[str, Any], *keys: str, default: Any = None) -> Any | None:
    for key in keys:
        obj = obj.get(key)  # type: ignore[assignment]
        if obj is None or len(obj) == 0:
            return default
        obj = obj[0]  # type: ignore[index]
    return obj


def get_ids(pm_info: dict[str, Any]) -> dict[str, str]:
    ids = {}
    for aid in select(pm_info, 'PubmedData', 'ArticleIdList', 'ArticleId', default=[]):  # type: ignore[union-attr]
        ids[aid['@IdType']] = aid['_text']
    return ids


def get_authors(citation: dict[str, Any]) -> Generator[AcademicAuthorModel, None, None]:
    authors = select(citation, 'Article', 'AuthorList', default={}).get('Author', [])  # type: ignore[union-attr]
    for author in authors:
        affiliations: list[AffiliationModel] = [
            AffiliationModel(name=select(aff, 'Affiliation', default={}).get('_text'))  # type: ignore[union-attr]
            for aff in author.get('AffiliationInfo', [])
        ]
        affiliations = [aff for aff in affiliations if aff.name is not None]

        yield AcademicAuthorModel(
            name=f'{select(author, "ForeName", default={}).get("_text", "")} '  # type: ignore[union-attr]
            f'{select(author, "LastName", default={}).get("_text", "")}',  # type: ignore[union-attr]
            affiliations=None if len(affiliations) == 0 else affiliations,
        )


class PubmedAPI(AbstractAPI):
    def _fetch_raw(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> Generator[Element, None, None]:
        """
        Pubmed API wrapper for downloading all records for a given query.

        Direct article lookup (IDs can be comma separated):
        https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?api_key=KEY&db=pubmed&id=17975326

        API Documentation:
        https://www.ncbi.nlm.nih.gov/books/NBK25497/
        https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.EFetch

        Works in two stages:
          1) Initiate query and get QueryKey
          2) Fetch result pages for QueryKey

        :param query:
        :return:
        """

        if self.api_key is None:
            raise AssertionError('Missing API key!')

        n_records = 0
        n_pages = 0
        with RequestClient(
            backoff_rate=self.backoff_rate,
            max_req_per_sec=self.max_req_per_sec,
            max_retries=self.max_retries,
            proxy=self.proxy,
        ) as request_client:
            search_page = request_client.post(
                'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi',
                data={
                    'api_key': self.api_key,
                    'db': 'pubmed',
                    'term': query,
                    'usehistory': 'y',
                },
                params=params,
            )
            tree = parse_xml(search_page.text)
            web_env = tree.find('WebEnv').text  # type: ignore[union-attr]
            query_key = tree.find('QueryKey').text  # type: ignore[union-attr]

            self.logger.warning(f'Query translated to: {tree.find("QueryTranslation").text}')  # type: ignore[union-attr]

            errors = tree.find('ErrorList')
            if errors is not None:
                for error in errors.iter():
                    self.logger.error(f'Error {error.tag}: {"".join(error.itertext())}')

            self.n_results = int(tree.find('Count').text)  # type: ignore[union-attr,arg-type]
            page_size = int(tree.find('RetMax').text)  # type: ignore[union-attr,arg-type]

            done = False

            def on_done(response: Response) -> dict[str, Any]:
                self.logger.info('Seemed to have reached the end (BAD_REQUEST).')
                done = True  # noqa: F841
                return {}

            request_client.on(codes.BAD_REQUEST, on_done)
            while not done:
                result_page = request_client.get(
                    'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi',
                    params={
                        'api_key': self.api_key,
                        'db': 'pubmed',
                        'WebEnv': web_env,
                        'query_key': query_key,
                        'retmax': page_size,
                        'retstart': n_records,
                    },
                )

                self.api_feedback = {
                    'rate_limit': result_page.headers.get('x-ratelimit-limit'),
                    'rate_left': result_page.headers.get('x-ratelimit-remaining'),
                }

                tree = parse_xml(result_page.text)
                articles = list(tree.findall('PubmedArticle'))

                n_records += len(articles)
                yield from articles

                self.logger.info(
                    f'Found {n_records:,}/{self.n_results:,} records after processing page {n_pages} ({page_size} per page) | {self.api_feedback}',
                )

                if n_records >= self.n_results or len(articles) == 0:
                    self.logger.info('Seemed to have reached the end (count zero or total reached).')
                    break

    def fetch_raw(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        for entry in self._fetch_raw(query, params=params):
            yield xml2dict(entry)

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        citation = record.get('MedlineCitation')[0]  # type: ignore[index]
        pm_info = record.get('PubmedData')[0]  # type: ignore[index]
        pm_ids = get_ids(pm_info)
        py: str | None = select(citation, 'Article', 'ArticleDate', 'Year', default={}).get('_text')  # type: ignore[union-attr]
        pyi: int | None = None if py is None else int(py)

        if 'ReferenceList' in pm_info:
            del pm_info['ReferenceList']

        return AcademicItemModel(
            item_id=uuid.uuid4(),
            project_id=as_uuid(project_id),
            doi=pm_ids.get('doi'),
            title=select(citation, 'Article', 'ArticleTitle', default={}).get('_text'),  # type: ignore[union-attr]
            pubmed_id=pm_ids.get('pubmed'),
            text=select(citation, 'Article', 'Abstract', 'AbstractText', default={}).get('_text'),  # type: ignore[union-attr]
            publication_year=pyi,
            authors=clear_empty(list(get_authors(citation))),
            source=select(citation, 'Article', 'Journal', 'Title', default={}).get('_text'),  # type: ignore[union-attr]
            keywords=clear_empty(
                [
                    kw.get('_text', '').strip()
                    for kw in select(citation, 'KeywordList', default={}).get('Keyword', [])  # type: ignore[union-attr]
                ],
            ),
            meta={'pubmed-api': clear_empty(record)},
        )


if __name__ == '__main__':
    app = PubmedAPI.test_app(
        static_files=[
            'scratch/academic_apis/response_pubmed.jsonl',
        ],
    )
    app()
