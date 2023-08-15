import json
import uuid
import logging
from datetime import timedelta
from pathlib import Path
from time import time
from typing import Generator, Literal

import httpx
from pydantic import BaseModel

from .duplicate import str_to_title_slug
from ...models.items import AcademicItemModel
from ...models.items.academic import AcademicAuthorModel, AffiliationModel
from ...models.openalex.solr import WorkSolr, Work, Location, Authorship, Biblio
from .clean import clear_empty

logger = logging.getLogger('nacsos_data.util.academic.openalex')


def translate_doc(doc: WorkSolr) -> Work:
    locations = None
    if doc.locations is not None:
        raw = json.loads(doc.locations)
        locations = [
            Location.model_validate(loc)
            for loc in raw
        ]
    authorships = None
    if doc.authorships is not None:
        raw = json.loads(doc.authorships)
        authorships = [
            Authorship.model_validate(loc)
            for loc in raw
        ]

    return Work.model_validate({
        **doc.model_dump(),
        'locations': locations,
        'authorships': authorships,
        'biblio': Biblio.model_validate(json.loads(doc.biblio)) if doc.biblio is not None else None
    })


def translate_authorship(author: Authorship) -> AcademicAuthorModel:
    ret = AcademicAuthorModel(name='[missing]')
    if author.author is not None:
        if author.author.display_name is not None:
            ret.name = author.author.display_name
        ret.openalex_id = author.author.id
        ret.orcid = author.author.orcid
    if author.institutions is not None:
        ret.affiliations = [
            AffiliationModel(name=inst.display_name if inst.display_name is not None else '[missing]',
                             openalex_id=inst.id,
                             country=inst.country_code)
            for inst in author.institutions
        ]
    elif author.raw_affiliation_string is not None:
        ret.affiliations = [AffiliationModel(name=author.raw_affiliation_string)]

    return ret


def translate_work(work: Work) -> AcademicItemModel:
    source = None
    if work.locations is not None and len(work.locations) > 0:
        # find the first location with a source name
        for loc in work.locations:
            if loc.source is not None and loc.source.display_name is not None:
                source = loc.source.display_name
                break

    doi: str | None = None
    if work.doi is not None:
        doi = work.doi.replace('https://doi.org/', '')

    return AcademicItemModel(
        item_id=None,
        doi=doi,
        openalex_id=work.id,
        pubmed_id=work.pmid,  # work.pmcid
        # wos_id
        # scopus_id
        # s2_id
        # dimensions_id
        title=work.title,
        title_slug=str_to_title_slug(work.title),
        text=work.abstract,
        publication_year=work.publication_year,
        source=source,
        # keywords
        authors=[translate_authorship(a) for a in work.authorships] if work.authorships is not None else None,

        meta=clear_empty({
            'openalex': {
                'locations': work.locations,
                'type': work.type,
                'updated_date': work.updated_date,
                'mag': work.mag,
                'pmid': work.pmid,
                'pmcid': work.pmcid,
                'display_name': work.display_name,
                'is_oa': work.is_oa,
                'is_paratext': work.is_paratext,
                'is_retracted': work.is_retracted,
                'language': work.language
            }
        })
    )


class SearchResult(BaseModel):
    query_time: int
    num_found: int
    docs: list[AcademicItemModel]
    histogram: dict[str, int] | None = None


async def get_async(url: str) -> SearchResult:
    async with httpx.AsyncClient() as client:
        request = await client.get(url)
        response = request.json()
        logger.debug(f'Received response from OpenAlex: {response["responseHeader"]}')
        return SearchResult(
            num_found=response['response']['numFound'],
            query_time=response['responseHeader']['QTime'],
            docs=[
                translate_work(translate_doc(WorkSolr.model_validate(d)))
                for d in response['response']['docs']
            ])


DefType = Literal['edismax', 'lucene', 'dismax']
SearchField = Literal['title', 'abstract', 'title_abstract']
OpType = Literal['OR', 'AND']


async def query_async(query: str,
                      openalex_endpoint: str,
                      limit: int = 20,
                      offset: int = 0,
                      def_type: DefType = 'lucene',
                      field: SearchField = 'title_abstract',
                      histogram: bool = False,
                      op: OpType = 'AND',
                      histogram_from: int = 1990,
                      histogram_to: int = 2024) -> SearchResult:
    params = {
        'q': query,
        'q.op': op,
        'useParams': '',
        'rows': limit,
        'start': offset
        # 'hl': 'true',
        # 'hl.fl': 'title,abstract',
    }

    if def_type == 'lucene':
        params['df'] = field
    else:
        params['defType'] = def_type
        params['qf'] = field

    if histogram:
        params.update({
            'facet.range': 'publication_year',
            'facet': 'true',
            'facet.sort': 'index',
            'facet.range.gap': '1',
            'facet.range.start': histogram_from,
            'facet.range.end': histogram_to
        })

    async with httpx.AsyncClient() as client:
        request = await client.post(str(openalex_endpoint), data=params)
        response = request.json()
        logger.debug(f'Received response from OpenAlex: {response["responseHeader"]}')

        hist_facets = (response
                       .get('facet_counts', {})
                       .get('facet_ranges', {})
                       .get('publication_year', {})
                       .get('counts', None))

        return SearchResult(
            num_found=response['response']['numFound'],
            query_time=response['responseHeader']['QTime'],
            docs=[
                translate_work(translate_doc(WorkSolr.model_validate(d)))
                for d in response['response']['docs']
            ],
            histogram={
                hist_facets[i]: hist_facets[i + 1]
                for i in range(0, len(hist_facets), 2)
            } if hist_facets is not None else None)


def generate_items_from_openalex(query: str,
                                 openalex_endpoint: str,
                                 export_fields: list[str] | None = None,
                                 batch_size: int = 10000,
                                 def_type: DefType = 'lucene',
                                 field: SearchField = 'title_abstract',
                                 op: OpType = 'AND',
                                 translate: bool = False) -> Generator[AcademicItemModel, None, None]:
    if export_fields is None:
        export_fields = [
            'id', 'title', 'abstract', 'display_name',  # 'title_abstract',
            'publication_year', 'publication_date',
            'created_date', 'updated_date',
            'cited_by_count', 'type', 'doi', 'mag', 'pmid', 'pmcid',
            'is_oa', 'is_paratext', 'is_retracted',
            'locations', 'authorships', 'biblio', 'language'
        ]

    params = {
        'q': query,
        'q.op': op,
        'sort': 'id desc',
        'fl': ','.join(export_fields),
        'rows': batch_size,
        'cursorMark': '*'
    }
    if def_type == 'lucene':
        params['df'] = field
    else:
        params['defType'] = def_type
        params['qf'] = field

    t0 = time()
    logger.info(f'Querying endpoint with batch_size={batch_size:,}: {openalex_endpoint}')

    batch_i = 0
    num_docs_cum = 0
    while True:
        t1 = time()
        batch_i += 1
        logger.info(f'Running query for batch {batch_i} with cursor "{params["cursorMark"]}"')
        t2 = time()
        res = httpx.post(openalex_endpoint, data=params).json()
        params['cursorMark'] = res['nextCursorMark']
        n_docs_total = res['response']['numFound']
        batch_docs = res['response']['docs']
        n_docs_batch = len(batch_docs)
        num_docs_cum += n_docs_batch

        logger.debug(f'Query took {timedelta(seconds=time() - t2)}h and yielded {n_docs_batch:,} docs')
        logger.debug(f'Current progress: {num_docs_cum:,}/{n_docs_total:,}={num_docs_cum / n_docs_total:.2%} docs')

        if len(batch_docs) == 0:
            logger.info('No documents in this batch, assuming to be done!')
            break

        if translate:
            logger.debug('Yielding translated documents...')
            yield from [translate_work(translate_doc(doc)) for doc in batch_docs]
        else:
            logger.debug('Yielding solr documents...')
            yield from batch_docs

        logger.debug(f'Done with batch {batch_i} in {timedelta(seconds=time() - t1)}h; '
                     f'{timedelta(seconds=time() - t0)}h passed overall')


def download_openalex_query(target_file: str | Path,
                            query: str,
                            openalex_endpoint: str,
                            export_fields: list[str] | None = None,
                            batch_size: int = 10000,
                            translate: bool = False) -> None:
    """
    This executes a `query` in solr at the specified `openalex_endpoint` (collection's select endpoint)
    and writes each document as one json string per line into `target_file`.

    You can specify the `batch_size` (how many documents per request)
    and which `export_fields` from the collection to get.

    :param query: solr query
    :param target_file:
    :param batch_size: Maximum number of documents to return per request
    :param translate: True=OpenAlex Work objects will be translated to AcademicItemModel before saving
    :param openalex_endpoint: sth like "http://[IP]:8983/solr/openalex"
    :param export_fields:
    :return:
    """
    # ensure the path to that file exists
    target_file = Path(target_file)
    target_file.parent.mkdir(exist_ok=True, parents=True)
    logger.info(f'Writing results to: {target_file}')

    with open(target_file, 'w') as f_out:
        [f_out.write(json.dumps(doc) + '\n') for doc in generate_items_from_openalex(
            query=query,
            openalex_endpoint=openalex_endpoint,
            translate=translate,
            batch_size=batch_size,
            export_fields=export_fields)]


def generate_items_from_openalex_export(openalex_export: str | Path,
                                        project_id: str | uuid.UUID | None,
                                        needs_translation: bool = False) -> Generator[AcademicItemModel, None, None]:
    """
    Assumes to get the path to a file produced by `download_openalex_query()` and will generate
    AcademicItems for each line in that file and associates this with `project_id`

    :param needs_translation:
    :param openalex_export:
    :param project_id:
    :return:
    """
    with open(openalex_export, 'r') as oa_file:
        for line in oa_file:
            if needs_translation:
                model = AcademicItemModel.model_validate_json(line)
            else:
                model = translate_work(translate_doc(WorkSolr.model_validate_json(line)))
            model.project_id = project_id
            yield model
