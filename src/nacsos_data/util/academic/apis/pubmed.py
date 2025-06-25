import uuid
from typing import Any, Generator
from xml.etree.ElementTree import Element, fromstring as parse_xml

from nacsos_data.models.items import AcademicItemModel
from nacsos_data.util import as_uuid
from nacsos_data.util.xml import xml2dict
from nacsos_data.util.academic.apis.util import RequestClient, AbstractAPI


def get_title(article: Element) -> str | None:
    hits = article.findall('.//ArticleTitle')
    if len(hits) > 0:
        return ' '.join(hits[0].itertext())
    return None


def get_abstract(article: Element) -> str | None:
    hits = article.findall('.//Abstract')
    if len(hits) > 0:
        return '\n\n'.join(hits[0].itertext())
    return None


def get_doi(article: Element) -> str | None:
    hits = article.findall('.//ArticleId[@IdType="doi"]')
    if len(hits) > 0:
        return hits[0].text
    return None


def get_id(article: Element) -> str | None:
    hits = article.findall('.//PMID')
    if len(hits) > 0:
        return hits[0].text
    return None


class PubmedAPI(AbstractAPI):

    def _fetch_raw(self, query: str) -> Generator[Element, None, None]:
        """
        Pubmed API wrapper for downloading all records for a given query.

        Direct article lookup (IDs can be comma separated):
        https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?api_key=KEY&db=pubmed&id=17975326

        API Documentation:
        https://www.ncbi.nlm.nih.gov/books/NBK25497/

        Works in two stages:
          1) Initiate query and get QueryKey
          2) Fetch result pages for QueryKey

        :param api_key:
        :param query:
        :param logger:
        :return:
        """
        n_records = 0
        n_pages = 0
        with RequestClient(timeout_rate=self.timeout_rate,
                           max_req_per_sec=self.max_req_per_sec,
                           max_retries=self.max_retries,
                           proxy=self.proxy) as request_client:
            search_page = request_client.get(
                'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi',
                params={
                    'api_key': self.api_key,
                    'db': 'pubmed',
                    'term': query,
                    'usehistory': 'y',
                },
            )
            tree = parse_xml(search_page.text)
            web_env = tree.find('WebEnv').text  # type: ignore[union-attr]
            query_key = tree.find('QueryKey').text  # type: ignore[union-attr]
            # TODO: get total result size

            while True:
                # FIXME: paginate
                result_page = request_client.get(
                    'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi',
                    params={
                        'api_key': self.api_key,
                        'db': 'pubmed',
                        'WebEnv': web_env,
                        'query_key': query_key,
                    },
                )

                tree = parse_xml(result_page.text)
                for article in tree.findall('PubmedArticle'):
                    yield article
                    n_records += 1
                self.logger.debug(f'Found {n_records:,} records after processing page {n_pages}')
                break  # FIXME

    def fetch_raw(self, query: str) -> Generator[dict[str, Any], None, None]:
        for entry in self._fetch_raw(query):
            yield xml2dict(entry)

    @classmethod
    def translate_record(cls, record: dict[str, Any], project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
        return AcademicItemModel(
            item_id=uuid.uuid4(),
            project_id=as_uuid(project_id),
            doi=get_doi(record),
            title=get_title(record),
            pubmed_id=get_id(record),
            text=get_abstract(record),
            # TODO
        )


if __name__ == '__main__':
    app = PubmedAPI.test_app(
        static_files=[
            # 'scratch/academic_apis/response_scopus1.json',
            # 'scratch/academic_apis/response_scopus2.jsonl',
        ])
    app()
