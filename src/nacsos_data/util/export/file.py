import csv
import tempfile
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
                }
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


def write_ris(
    result: list[dict[str, Any]],
    labels: list[LabelOptions],
    label_mappings: dict[str, tuple[str, str]],
    label_format: RISLabelFormat,
) -> str:
    def _prepare_record(row: dict[str, Any]) -> dict[str, Any] | None:
        label_tags = _get_label_tags(
            labels=labels,
            row=row,
            label_mappings=label_mappings,
            label_format=label_format,
        )

        keywords = label_tags
        if row.get('keywords') is not None and len(row.get('keywords', [])) > 0:
            keywords += row.get('keywords', [])

        note = ''
        if row.get('openalex_id'):
            note += f'OpenAlex ID: {row.get("openalex_id")}\n'
        if row.get('item_id'):
            note += f'NACSOS ID: {row.get("item_id")}\n'
        if row.get('username'):
            note += f'Annotated by: {row.get("username")}\n'
        if label_tags:
            note += 'Annotations: ' + ', '.join(label_tags)

        out = {
            # In prod academic_items; up to 2.5M out of 8M missing for these columns; so to make it error prone, default to empty string
            'abstract': row.get('text'),
            'title': row.get('title'),
            'doi': f'https://doi.org/{row.get("doi")}',
            'custom1': row.get('openalex_id'),
            'custom2': str(row.get('item_id')),
            'year': row.get('publication_year'),
            'journal_name': row.get('source'),
            'authors': row.get('authors', []),
            'keywords': keywords,
            'label': label_tags,
            'notes': [note.strip()],
        }

        return clear_empty(out)

    with tempfile.NamedTemporaryFile(suffix='.ris', mode='w', delete=False) as fp:
        rispy.dump(references=[_prepare_record(row) for row in result], file=fp)  # pyright: ignore[reportArgumentType]

        return fp.name


def write_jsonl(result: list[dict[str, Any]]) -> str:
    encoder = DictLikeEncoder()
    encoded_result = [encoder.encode(row) for row in result]

    with tempfile.NamedTemporaryFile(suffix='.jsonl', mode='w', delete=False) as fp:
        [fp.write(row + '\n') for row in encoded_result]

    return fp.name
