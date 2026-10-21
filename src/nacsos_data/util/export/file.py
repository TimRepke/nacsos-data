import csv
import tempfile
from collections import defaultdict

import rispy
import xlsxwriter

from typing import Any

from nacsos_data.db.engine import DictLikeEncoder
from nacsos_data.util.export.util import LabelOptions, RISLabelFormat, encode_excel
from nacsos_data.util import clear_empty

DEFAULT_COLUMNS_TO_DROP = ['type', 'time_edited', 'project_id', 'title_slug', 'keywords', 'meta']


def get_author_names(authors: Any) -> list[str]:
    """Works within a single row from output of from nacsos_data.util.export.dict prepare_export_table"""
    if authors is None or not authors or not isinstance(authors, list):
        return []

    first = authors[0]
    # academic item
    if isinstance(first, dict) and 'name' in first.keys():
        return [a.get('name') for a in authors]
    # lexis item
    if isinstance(first, str):
        return authors
    return []


def write_csv(result: list[dict[str, Any]]) -> str:
    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w', newline='', delete=False) as fp:
        writer = csv.DictWriter(fp, fieldnames=list(result[0].keys()))
        writer.writeheader()
        for row in result:
            writer.writerow(
                row
                | {
                    k: v
                    for k, v in {
                        'authors': '; '.join((row.get('authors') or [])),
                        'keywords': '; '.join((row.get('keywords') or [])),
                    }.items()
                    if k in row
                },
            )
    return fp.name


def write_excel(result: list[dict[str, Any]]) -> str:
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.xlsx', mode='wb', delete=False) as fp:
        temp_path = fp.name

    # Create a workbook and worksheet
    wb = xlsxwriter.Workbook(temp_path)
    ws = wb.add_worksheet('NACSOS Annotations Export')

    # Write header row
    headers = list(result[0].keys())

    ws.write_row(0, 0, headers)

    # Write data rows
    for row_idx, row in enumerate(result, start=1):
        ws.write_row(row_idx, 0, [encode_excel(row.get(h)) for h in headers])

    # Set column widths
    for col_idx, header in enumerate(headers):
        ws.set_column(col_idx, col_idx, max(20, len(str(header)) + 2))

    # Freeze header row
    ws.freeze_panes(1, 0)
    # Close the workbook to flush all data
    wb.close()

    return temp_path


def _get_label_tags(
    labels: list[LabelOptions],
    row: dict[str, Any],
    label_mappings: dict[str, tuple[str, str]],
    label_format: RISLabelFormat,
) -> list[str]:
    label_tags: list[str] = []

    def add_tags(
        label_: LabelOptions,
        choices: list[int] | None,
        include_category: bool,
    ) -> None:
        if choices is None:
            return

        for choice in choices:
            label_key = f'{label_.key}|{choice}'

            if row.get(label_key) != 1:
                continue

            if label_format is RISLabelFormat.RAW_TAGS:
                label_tags.append(label_key)
                continue

            label_category, label_name = label_mappings.get(
                label_key,
                ('', ''),
            )

            if label_format is RISLabelFormat.LABEL_AND_CHOICE_NAMES and include_category:
                label_tags.append(f'{label_category}: {label_name}')
            else:
                label_tags.append(label_name)

    for label in labels:
        add_tags(label, label.options_int, include_category=True)

    for label in labels:
        add_tags(label, label.options_multi, include_category=True)

    for label in labels:
        bool_options = [0, 1] if label.options_bool is not None else None
        add_tags(label, bool_options, include_category=False)

    return label_tags


def write_ris(  # noqa: C901
    result: list[dict[str, Any]],
    labels: list[LabelOptions],
    label_mappings: dict[str, tuple[str, str]],
    label_format: RISLabelFormat,
) -> str:
    grouped_results = defaultdict(list)
    for row in result:
        grouped_results[row['item_id']].append(row)

    def _prepare_record(records: list[dict[str, Any]]) -> dict[str, Any]:  # noqa: C901
        if len(records) == 0:
            raise AssertionError('Record group is empty, this should never happen!')

        label_tags = {
            record.get('username', 'RESOLVED'): _get_label_tags(
                labels=labels,
                row=record,
                label_mappings=label_mappings,
                label_format=label_format,
            )
            for record in records
        }
        label_tags = {k or 'UNKNOWN': v for k, v in label_tags.items() if v is not None and len(v) > 0}

        if 'RESOLVED' in label_tags:
            flat_tags = label_tags['RESOLVED']
        else:
            flat_tags = list({tag for tags in label_tags.values() for tag in tags})

        record = records[0]

        keywords = flat_tags
        if record.get('keywords') is not None and len(record.get('keywords', [])) > 0:
            keywords += record.get('keywords', [])

        note = ''
        if record.get('openalex_id'):
            note += f'OpenAlex ID: {record.get("openalex_id")}\n'
        if record.get('item_id'):
            note += f'NACSOS ID: {record.get("item_id")}\n'
        if record.get('scopus_id'):
            note += f'Scopus ID: {record.get("scopus_id")}\n'
        if record.get('wos_id'):
            note += f'WoS ID: {record.get("wos_id")}\n'
        if record.get('pubmed_id'):
            note += f'pubmed ID: {record.get("pubmed_id")}\n'
        if record.get('dimensions_id'):
            note += f'Dimensions ID: {record.get("dimensions_id")}\n'

        if len(label_tags) > 0:
            for user, annotations in label_tags.items():
                note += f'\nAnnotation by {user}: {", ".join(annotations)}\n'

        out = {
            # In prod academic_items; up to 2.5M out of 8M missing for these columns; so to make it error prone, default to empty string
            'abstract': record.get('text'),
            'title': record.get('title'),
            'doi': f'https://doi.org/{record.get("doi")}' if record.get('doi') else None,
            'custom1': record.get('openalex_id'),
            'custom2': str(record.get('item_id')),
            'year': record.get('publication_year'),
            'journal_name': record.get('source'),
            'authors': record.get('authors', []),
            'keywords': keywords,
            'label': flat_tags,
            'notes': [note.strip()],
        }

        res: dict[str, Any] | None = clear_empty(out)

        if res is None:
            raise AssertionError('Record is empty, this should never happen!')
        return res

    with tempfile.NamedTemporaryFile(suffix='.ris', mode='w', delete=False) as fp:
        rispy.dump(
            references=[_prepare_record(group) for group in grouped_results.values()],
            file=fp,
        )

        return fp.name


def write_jsonl(result: list[dict[str, Any]]) -> str:
    encoder = DictLikeEncoder()
    encoded_result = [encoder.encode(row) for row in result]

    with tempfile.NamedTemporaryFile(suffix='.jsonl', mode='w', delete=False) as fp:
        [fp.write(row + '\n') for row in encoded_result]

    return fp.name
