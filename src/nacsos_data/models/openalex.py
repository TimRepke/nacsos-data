import re
from typing import TypeVar, Annotated, Literal, Any

from pydantic import BaseModel, BeforeValidator, model_validator, AfterValidator


# based on
# https://github.com/ourresearch/openalex-elastic-api/blob/master/works/schemas.py
# https://github.com/ourresearch/openalex-elastic-api/blob/master/core/schemas.py


DefType = Literal['edismax', 'lucene', 'dismax']
SearchField = Literal['title', 'abstract', 'title_abstract']
OpType = Literal['OR', 'AND']
AbstractSource = Literal['OpenAlex', 'Deprecated', 'Scopus', 'WoS', 'Pubmed', 'Other']

T = TypeVar('T')


def ensure_clean_list(v: list[T] | None) -> list[T] | None:
    if v is None or len(v) == 0 or v[0] is None:
        return None
    return v


URLS = re.compile(
    r'(https://openalex.org/'
    r'|https://orcid.org/'
    r'|https://doi.org/'
    r'|https://www.wikidata.org/wiki/'
    r'|https://ror.org/)'
)


def strip_url(url: str | None) -> str | None:
    if url is None:
        return None
    return URLS.sub('', url)


def strip_urls(urls: list[str] | None) -> list[str] | None:
    if urls is None or len(urls) == 0:
        return None
    return [URLS.sub('', url) for url in urls]


def invert_abstract(abstract_inverted_index: dict[str, list[int]] | None) -> str | None:
    if abstract_inverted_index is None:
        return None

    token: str
    position: int
    positions: list[int]

    abstract_length: int = sum([len(idxs) for idxs in abstract_inverted_index.values()])
    abstract: list[str] = [''] * abstract_length

    for token, positions in abstract_inverted_index.items():
        for position in positions:
            if position < abstract_length:
                abstract[position] = token

    return ' '.join(abstract)


class MetaSchema(BaseModel, extra='allow'):
    count: int | None = None
    q: str | None = None
    db_response_time_ms: int | None = None
    page: int | None = None
    per_page: int | None = None
    next_cursor: str | None = None
    groups_count: int | None = None
    apc_list_sum_usd: int | None = None
    apc_paid_sum_usd: int | None = None
    cited_by_count_sum: int | None = None


class CountsByYearSchema(BaseModel, extra='allow'):
    year: int | None = None
    works_count: int | None = None
    oa_works_count: int | None = None
    cited_by_count: int | None = None


class XConceptsSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    wikidata: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    level: int | None = None
    score: float | None = None


class SummaryStatsSchema(BaseModel, extra='allow'):
    two_mean_citedness: float | None = None
    h_index: int | None = None
    i10_index: int | None = None


class ValuesSchema(BaseModel, extra='allow'):
    value: str | None = None
    display_name: str | None = None
    count: int | None = None
    url: str | None = None
    db_response_time_ms: int | None = None


class TopicHierarchySchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None


class TopicSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    count: int | None = None
    value: float | None = None
    score: float | None = None
    subfield: TopicHierarchySchema | None = None
    field: TopicHierarchySchema | None = None
    domain: TopicHierarchySchema | None = None


class RolesSchema(BaseModel, extra='allow'):
    role: str | None = None
    id: str | None = None
    works_count: int | None = None


class PercentilesSchema(BaseModel, extra='allow'):
    percentile: float | None = None
    value: float | None = None


class StatsMetaSchema(BaseModel, extra='allow'):
    count: int | None = None
    entity: str | None = None
    # filters :list[fields.Dict(]|None=None)
    search: str | None = None
    db_response_time_ms: int | None = None


class StatsSchema(BaseModel, extra='allow'):
    key: str | None = None
    percentiles: dict[int, int] | None = None
    sum: int | None = None


class StatsWrapperSchema(BaseModel, extra='allow'):
    meta: StatsMetaSchema | None = None
    stats: list[StatsSchema] | None = None


class AuthorSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    orcid: Annotated[str | None, AfterValidator(strip_url)] = None


class InstitutionsSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    ror: Annotated[str | None, AfterValidator(strip_url)] = None
    country_code: str | None = None
    type: str | None = None
    lineage: Annotated[list[str] | None, AfterValidator(strip_urls)] = None


class AffiliationsSchema(BaseModel, extra='allow'):
    raw_affiliation_string: str | None = None
    institution_ids: Annotated[list[str] | None, AfterValidator(strip_urls)] = None


class AuthorshipsSchema(BaseModel, extra='allow'):
    author_position: str | None = None
    author: AuthorSchema | None = None
    institutions: Annotated[list[InstitutionsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    countries: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    is_corresponding: bool | None = None
    raw_author_name: str | None = None
    raw_affiliation_strings: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    affiliations: Annotated[list[AffiliationsSchema] | None, BeforeValidator(ensure_clean_list)] = None


class APCSchema(BaseModel, extra='allow'):
    value: int | None = None
    price: int | None = None
    currency: str | None = None
    value_usd: int | None = None
    provenance: str | None = None


class BiblioSchema(BaseModel, extra='allow'):
    volume: str | None = None
    issue: str | None = None
    first_page: str | None = None
    last_page: str | None = None


class ConceptsSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    wikidata: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    level: int | None = None
    score: float | None = None


class CitationNormalizedPercentileSchema(BaseModel, extra='allow'):
    value: float | None = None
    is_in_top_1_percent: bool | None = None
    is_in_top_10_percent: bool | None = None


class GrantsSchema(BaseModel, extra='allow'):
    funder: Annotated[str | None, AfterValidator(strip_url)] = None
    funder_display_name: str | None = None
    award_id: str | None = None


class HasContentSchema(BaseModel, extra='allow'):
    pdf: bool | None = None
    grobid_xml: bool | None = None


class AwardsSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    funder_award_id: str | None = None
    funder_id: Annotated[str | None, AfterValidator(strip_url)] = None
    funder_display_name: str | None = None
    doi: Annotated[str | None, AfterValidator(strip_url)] = None


class FundersSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    ror: Annotated[str | None, AfterValidator(strip_url)] = None


class SourceSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    issn_l: str | None = None
    issn: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    is_oa: bool | None = None
    is_in_doaj: bool | None = None
    is_indexed_in_scopus: bool | None = None
    is_core: bool | None = None
    host_organization: Annotated[str | None, AfterValidator(strip_url)] = None
    host_organization_name: str | None = None
    host_organization_lineage: Annotated[list[str] | None, BeforeValidator(ensure_clean_list), AfterValidator(strip_urls)] = None
    host_organization_lineage_names: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    type: str | None = None
    raw_type: str | None = None


class HostOrganizationSchema(BaseModel, extra='allow'):
    """
    New schema for Walden, replace host_organization in locations.
    """

    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None


class SourcesLocationsSchema(BaseModel, extra='allow'):
    """
    New schema for Walden, replaces locations.
    """

    url: str | None = None
    content_type: str | None = None


class SourcesSchema(BaseModel, extra='allow'):
    """
    New schema for Walden, replaces locations.
    """

    native_id: Annotated[str | None, AfterValidator(strip_url)] = None
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    locations: Annotated[list[SourcesLocationsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    issn_l: str | None = None
    issns: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] | None = None
    is_oa: bool | None = None
    is_in_doaj: bool | None = None
    is_core: bool | None = None
    host_organization: HostOrganizationSchema | None = None
    type: str | None = None


class LocationSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    is_oa: bool | None = None
    landing_page_url: str | None = None
    pdf_url: str | None = None
    source: SourceSchema | None = None
    license: str | None = None
    license_id: Annotated[str | None, AfterValidator(strip_url)] = None
    version: str | None = None
    is_accepted: bool | None = None
    is_published: bool | None = None
    is_primary: bool | None = None
    raw_source_name: str | None = None
    raw_type: str | None = None

    # primary_location attributes
    native_id: Annotated[str | None, AfterValidator(strip_url)] = None
    provenance: str | None = None
    oa_status: int | None = None
    apc_prices: Annotated[list[APCSchema] | None, BeforeValidator(ensure_clean_list)] = None
    apc_usd: int | None = None
    host_type: str | None = None
    is_unpaywall_record: bool | None = None
    location_type: str | None = None
    updated: str | None = None
    # sort_score: int
    # url_sort_score: int


class OpenAccessSchema(BaseModel, extra='allow'):
    is_oa: bool | None = None
    oa_status: str | None = None
    oa_url: str | None = None
    any_repository_has_fulltext: bool | None = None


class IDsSchema(BaseModel, extra='allow'):
    openalex: Annotated[str | None, AfterValidator(strip_url)] = None
    doi: Annotated[str | None, AfterValidator(strip_url)] = None
    mag: str | None = None
    pmid: str | None = None
    pmcid: str | None = None


class MeshSchema(BaseModel, extra='allow'):
    descriptor_ui: str | None = None
    descriptor_name: str | None = None
    qualifier_ui: str | None = None
    qualifier_name: str | None = None
    is_major_topic: bool | None = None


class SDGSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    score: float | None = None


class CitedByPercentileYearSchema(BaseModel, extra='allow'):
    min: int | None = None
    max: int | None = None


class KeywordsSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    display_name: str | None = None
    score: float | None = None


NON_ALPHA = re.compile(r'[^a-zA-Z0-9]+')


class WorksSchema(BaseModel, extra='allow'):
    id: Annotated[str | None, AfterValidator(strip_url)] = None
    doi: Annotated[str | None, AfterValidator(strip_url)] = None
    title: str | None = None
    display_name: str | None = None
    publication_year: int | None = None
    publication_date: str | None = None
    ids: IDsSchema | None = None
    language: str | None = None
    primary_location: LocationSchema | None = None
    sources: Annotated[list[SourcesSchema] | None, BeforeValidator(ensure_clean_list)] = None
    type: str | None = None
    type_crossref: str | None = None
    indexed_in: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    open_access: OpenAccessSchema | None = None
    authorships: Annotated[list[AuthorshipsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    institution_assertions: Annotated[list[InstitutionsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    institutions: Annotated[list[InstitutionsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    countries_distinct_count: int | None = None
    institutions_distinct_count: int | None = None
    corresponding_author_ids: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    corresponding_institution_ids: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    apc_list: APCSchema | None = None
    apc_paid: APCSchema | None = None
    fwci: float | None = None
    # is_authors_truncated = fields.Bool(attribute="authorships_truncated")
    has_fulltext: bool | None = None
    fulltext_origin: str | None = None
    fulltext: str | None = None
    cited_by_count: int | None = None
    citation_normalized_percentile: CitationNormalizedPercentileSchema | None = None
    cited_by_percentile_year: CitedByPercentileYearSchema | None = None
    biblio: BiblioSchema | None = None
    is_retracted: bool | None = None
    is_paratext: bool | None = None
    is_xpac: bool | None = None
    primary_topic: TopicSchema | None = None
    topics: Annotated[list[TopicSchema] | None, BeforeValidator(ensure_clean_list)] = None
    keywords: Annotated[list[KeywordsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    concepts: Annotated[list[ConceptsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    mesh: Annotated[list[MeshSchema] | None, BeforeValidator(ensure_clean_list)] = None
    locations_count: int | None = None
    locations: Annotated[list[LocationSchema] | None, BeforeValidator(ensure_clean_list)] = None
    best_oa_location: LocationSchema | None = None
    sustainable_development_goals: Annotated[list[SDGSchema] | None, BeforeValidator(ensure_clean_list)] = None
    grants: Annotated[list[GrantsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    awards: Annotated[list[AwardsSchema] | None, BeforeValidator(ensure_clean_list)] = None
    funders: Annotated[list[FundersSchema] | None, BeforeValidator(ensure_clean_list)] = None
    datasets: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    versions: Annotated[list[str] | None, BeforeValidator(ensure_clean_list)] = None
    has_content: HasContentSchema | None = None
    referenced_works_count: int | None = None
    referenced_works: Annotated[list[str] | None, BeforeValidator(ensure_clean_list), AfterValidator(strip_urls)] = None
    related_works: Annotated[list[str] | None, BeforeValidator(ensure_clean_list), AfterValidator(strip_urls)] = None
    cited_by_api_url: str | None = None
    counts_by_year: Annotated[list[CountsByYearSchema] | None, BeforeValidator(ensure_clean_list)] = None
    updated_date: str | None = None
    created_date: str | None = None

    # Custom attributes
    abstract: str | None = None
    title_abstract: str | None = None
    abstract_inverted_index: dict[str, list[int]] | None = None
    abstract_source: AbstractSource | None = None

    is_published: bool | None = None
    is_accepted: bool | None = None
    is_open_access: bool | None = None

    publisher: str | None = None
    publisher_id: Annotated[str | None, AfterValidator(strip_url)] = None
    source: str | None = None
    source_id: Annotated[str | None, AfterValidator(strip_url)] = None

    id_mag: str | None = None
    id_pmid: str | None = None
    id_pmcid: str | None = None

    @model_validator(mode='before')
    @classmethod
    def _custom(cls, data: dict[str, Any]) -> dict[str, Any]:
        if data.get('primary_location') and data.get('locations') and len(data['locations']) > 0:
            data['locations'][0] = data['locations'][0] | data['primary_location'] | {'is_primary': True}

            if data['primary_location'].get('source', {}).get('display_name'):
                data['source'] = data['primary_location']['source']['display_name']
                data['source_id'] = data['primary_location']['source']['id']
            if data['primary_location'].get('source', {}).get('host_organization_name'):
                data['publisher'] = data['primary_location']['source']['host_organization_name']
                data['publisher_id'] = data['primary_location']['source']['host_organization']

            data['is_published'] = data['primary_location'].get('is_published', False)
            data['is_accepted'] = data['primary_location'].get('is_accepted', False)

        if data.get('ids'):
            if data['ids'].get('mag'):
                data['id_mag'] = str(data['ids']['mag'])
            if data['ids'].get('pmid'):
                data['id_pmid'] = str(data['ids']['pmid'])
            if data['ids'].get('pmcid'):
                data['id_pmcid'] = str(data['ids']['pmcid'])

        if not data.get('abstract') and data.get('abstract_inverted_index'):
            data['abstract'] = invert_abstract(data['abstract_inverted_index'])

        data['is_open_access'] = data.get('open_access', {}).get('is_oa', False)

        return data

    @property
    def tiab(self) -> str:
        return NON_ALPHA.sub(' ', f'{self.title or ""} {self.abstract or ""}')


if __name__ == '__main__':
    with open('../../../scratch/snippet.jsonl') as f:
        for li, line in enumerate(f):
            print(li)
            work = WorksSchema.model_validate_json(line)
            print(work.model_dump(exclude_unset=True, exclude_none=True))
            # print(work.model_dump())
