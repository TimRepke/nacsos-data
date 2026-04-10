import json
import uuid
from typing import get_args


from nacsos_data.models.items import AcademicItemModel
from nacsos_data.models.items.academic import AcademicAuthorModel, AffiliationModel
from nacsos_data.models.openalex import WorksSchema, AuthorshipsSchema, AbstractSource
from nacsos_data.util import clear_empty, as_uuid

FIELDS_API = {
    'id',
    'doi',
    'title',
    'display_name',
    # 'relevance_score',
    'publication_year',
    'publication_date',
    'ids',
    'language',
    'primary_location',
    # 'sources',
    'type',
    'type_crossref',
    'indexed_in',
    'open_access',
    'authorships',
    # 'institution_assertions',
    # 'institutions',
    # 'countries_distinct_count',
    # 'institutions_distinct_count',
    # 'corresponding_author_ids',
    # 'corresponding_institution_ids',
    # 'apc_list',
    # 'apc_paid',
    'fwci',
    # 'is_authors_truncated',
    'has_fulltext',
    # 'fulltext_origin',
    'cited_by_count',
    # 'citation_normalized_percentile',
    # 'cited_by_percentile_year',
    # 'biblio',
    'is_retracted',
    'is_paratext',
    'is_xpac',
    # 'primary_topic',
    'topics',
    'keywords',
    'concepts',
    # 'mesh',
    # 'locations_count',
    'locations',
    # 'best_oa_location',
    'sustainable_development_goals',
    'awards',
    'funders',
    # 'datasets',
    # 'versions',
    'has_content',
    # 'content_urls',
    # 'referenced_works_count',
    'referenced_works',
    # 'related_works',
    'abstract_inverted_index',
    # 'cited_by_api_url',
    # 'counts_by_year',
    'updated_date',
    'created_date',
}

FIELDS_REDUNDANT: set[str] = {
    'abstract_inverted_index',
    'ids',
    'display_name',
    'primary_location',
    'open_access',
    'has_content',
}

FIELDS_CUSTOM: set[str] = {
    'abstract',
    'title_abstract',
    'abstract_source',
    'is_published',
    'is_accepted',
    'is_open_access',
    'publisher',
    'publisher_id',
    'source',
    'source_id',
    'id_mag',
    'id_pmid',
    'id_pmcid',
    'has_pdf',
    'has_grobid_xml',
    'open_access_status',
    'any_repository_has_fulltext',
}

FIELDS_SOLR: set[str] = (FIELDS_API - FIELDS_REDUNDANT) | FIELDS_CUSTOM
FIELDS_META = set(FIELDS_SOLR) - {'abstract', 'title_abstract'}

NESTED_FIELDS = {field for field, dtype in WorksSchema.model_fields.items() if get_args(dtype.annotation)[0] not in {str, int, float, bool, AbstractSource}}


def translate_work_to_solr(work: WorksSchema, source: str = 'OpenAlex', authorship_limit: int | None = None) -> dict[str, str | bool | int | float]:
    doc = work.model_dump(include=FIELDS_SOLR, exclude_none=True, exclude_unset=True)

    if doc.get('authorships') and authorship_limit:
        doc['authorships'] = doc['authorships'][:authorship_limit]

    return (
        doc  # type: ignore[return-value]
        | {
            'title_abstract': work.tiab,
            'abstract_source': source if work.abstract else None,
        }
        | {field: json.dumps(doc[field]) for field in NESTED_FIELDS if field in doc}
    )


def translate_authorship(author: AuthorshipsSchema) -> AcademicAuthorModel:
    ret = AcademicAuthorModel(name='[missing]')
    if author.author is not None:
        if author.author.display_name is not None:
            ret.name = author.author.display_name
        ret.openalex_id = author.author.id
        ret.orcid = author.author.orcid
    if author.institutions is not None:
        ret.affiliations = [
            AffiliationModel(name=inst.display_name if inst.display_name is not None else '[missing]', openalex_id=inst.id, country=inst.country_code)
            for inst in author.institutions
        ]
    elif author.raw_affiliation_strings is not None:
        ret.affiliations = [AffiliationModel(name=affil) for affil in author.raw_affiliation_strings]

    return ret


def translate_work_to_item(work: WorksSchema, project_id: str | uuid.UUID | None = None) -> AcademicItemModel:
    source = None
    if work.locations is not None and len(work.locations) > 0:
        # find the first location with a source name
        for loc in work.locations:
            if loc.source is not None and loc.source.display_name is not None:
                source = loc.source.display_name
                break

    return AcademicItemModel(
        item_id=uuid.uuid4(),
        doi=work.doi,
        project_id=as_uuid(project_id),
        openalex_id=work.id,
        pubmed_id=work.id_pmid,
        title=work.title,
        text=work.abstract,
        publication_year=work.publication_year,
        source=source,
        authors=[translate_authorship(a) for a in work.authorships] if work.authorships is not None else None,
        meta=clear_empty(
            {
                'openalex': work.model_dump(include=FIELDS_META, exclude_none=True, exclude_unset=True),
            },
        ),
    )
