import csv
import tempfile
import rispy
import xlsxwriter

from typing import Any

from nacsos_data.db.engine import DictLikeEncoder
from nacsos_data.util.export.util import LabelOptions, encode_excel


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
    display_label_category: bool,
) -> list[str]:
    label_tags = []

    def add_tags(
        label_: LabelOptions,
        choices: list[int] | None,
        include_category: bool,
    ) -> None:
        if options is None:
            return

        for option in options:
            label_key = f'{label.key}|{option}'

            if row.get(label_key) != 1:
                continue

            label_category, label_name = label_mappings.get(
                label_key,
                ('', ''),
            )

            if display_label_category and include_category:
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
    display_label_category: bool = True,
) -> str:
    def _prepare_record(row: dict[str, Any]) -> dict[str, Any]:
        label_tags = _get_label_tags(
            labels=labels,
            row=row,
            label_mappings=label_mappings,
            display_label_category=display_label_category,
        )

        keywords = label_tags
        if row.get('keywords') is not None and len(row.get('keywords', [])) > 0:
            keywords += row.get('keywords', [])

        return {
            # In prod academic_items; up to 2.5M out of 8M missing for these columns; so to make it error prone, default to empty string
            'abstract': row.get('text', ''),
            'title': row.get('title', ''),
            'doi': f'https://doi.org/{row.get("doi", "") or ""}',
            'custom1': row.get('openalex_id', ''),
            'custom2': str(row.get('item_id', '')),
            'year': row.get('publication_year', ''),
            'journal_name': row.get('source', ''),
            'authors': row.get('authors', ''),
            'keywords': keywords,
            'label': label_tags or '',
            'notes': [
                f'openalex: {row.get("openalex_id", "") or ""}\nnacsos: {row.get("item_id", "") or ""}\nannotated by: {row.get("username", "") or ""}\nAnnotations: '
                + ', '.join(label_tags)
            ],
        }

    with tempfile.NamedTemporaryFile(suffix='.ris', mode='w', delete=False) as fp:
        rispy.dump(references=[_prepare_record(row) for row in result], file=fp)  # pyright: ignore[reportArgumentType]

        return fp.name


def write_jsonl(result: list[dict[str, Any]]) -> str:
    encoder = DictLikeEncoder()
    encoded_result = [encoder.encode(row) for row in result]

    with tempfile.NamedTemporaryFile(suffix='.jsonl', mode='w', delete=False) as fp:
        [fp.write(row + '\n') for row in encoded_result]

    return fp.name
