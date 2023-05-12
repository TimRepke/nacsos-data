import json
import uuid
import logging
from datetime import timedelta
from pathlib import Path
from time import time
from typing import Generator

import requests as requests

from ...models.items import AcademicItemModel
from .clean import clear_empty

logger = logging.getLogger('nacsos_data.util.academic.openalex')


def download_openalex_query(target_file: str | Path,
                            query: str,
                            openalex_endpoint: str,
                            batch_size: int = 10000,
                            export_fields: list[str] | None = None) -> None:
    """
    This executes a `query` in solr at the specified `openalex_endpoint` (collection's select endpoint)
    and writes each document as one json string per line into `target_file`.

    You can specify the `batch_size` (how many documents per request)
    and which `export_fields` from the collection to get.

    :param query:
    :param target_file:
    :param batch_size:
    :param openalex_endpoint: sth like "http://[IP]:8983/solr/openalex/select"
    :param export_fields:
    :return:
    """
    if export_fields is None:
        export_fields = [  # FIXME: extend the list once we have the full corpus
            'id', 'title', 'abstract', 'mag',
            'publication_year', 'cited_by_count', 'type', 'doi'
        ]

    # ensure the path to that file exists
    target_file = Path(target_file)
    target_file.parent.mkdir(exist_ok=True, parents=True)

    data = {
        'q': query,
        'q.op': 'AND',
        'df': 'ta',
        'sort': 'id desc',
        'fl': ','.join(export_fields),
        'rows': batch_size,
        'cursorMark': '*'
    }

    logger.info(f'Querying endpoint with batch_size={batch_size:,}: {openalex_endpoint}')
    logger.info(f'Writing results to: {target_file}')

    with open(target_file, 'w') as f_out:
        t0 = time()

        batch_i = 0
        num_docs_cum = 0
        while True:
            t1 = time()
            batch_i += 1
            logger.info(f'Running query for batch {batch_i} with cursor "{data["cursorMark"]}"')
            t2 = time()
            res = requests.post(openalex_endpoint, data=data).json()
            data['cursorMark'] = res['nextCursorMark']
            n_docs_total = res['response']['numFound']
            batch_docs = res['response']['docs']
            n_docs_batch = len(batch_docs)
            num_docs_cum += n_docs_batch

            logger.debug(f'Query took {timedelta(seconds=time() - t2)}h and yielded {n_docs_batch:,} docs')
            logger.debug(f'Current progress: {num_docs_cum:,}/{n_docs_total:,}={num_docs_cum / n_docs_total:.2%} docs')

            if len(batch_docs) == 0:
                logger.info('No documents in this batch, assuming to be done!')
                break

            logger.debug('Writing documents to file...')
            [f_out.write(json.dumps(doc) + '\n') for doc in batch_docs]

            logger.debug(f'Done with batch {batch_i} in {timedelta(seconds=time() - t1)}h; '
                         f'{timedelta(seconds=time() - t0)}h passed overall')


def generate_items_from_openalex(openalex_export: str,
                                 project_id: str | uuid.UUID | None) -> Generator[AcademicItemModel, None, None]:
    """
    Assumes to get the path to a file produced by `download_openalex_query()` and will generate
    AcademicItems for each line in that file and associates this with `project_id`

    :param openalex_export:
    :param project_id:
    :return:
    """
    with open(openalex_export, 'r') as oa_file:
        for line in oa_file:
            doc_ = json.loads(line)
            doi: str | None = doc_.get('doi')
            if doi is not None:
                doi = doi.replace('https://doi.org/', '')

            meta = {
                'cited_by_count': doc_.get('cited_by_count'),
                'publication_date': doc_.get('publication_date'),
                'type': doc_.get('type')
            }

            yield AcademicItemModel(  # FIXME: extend once we have the full corpus
                project_id=project_id,
                openalex_id=doc_.get('id'),
                doi=doi,
                title=doc_.get('title'),
                text=doc_.get('abstract'),
                publication_year=doc_.get('publication_year'),
                meta=clear_empty(meta)
            )
