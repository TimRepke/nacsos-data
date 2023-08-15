from typing import Any, Literal

from nacsos_data.util.academic.openalex import DefType, SearchField, OpType
from .abc import TaskParams


class OpenAlexImport(TaskParams):
    func_name: Literal['nacsos_lib.academic.import.import_openalex']  # type: ignore[misc]

    query: str
    def_type: DefType = 'lucene',
    field: SearchField = 'title_abstract',
    op: OpType = 'AND',
    project_id: str | None = None
    import_id: str | None = None

    @property
    def payload(self) -> dict[str, Any]:
        return {
            'project_id': str(self.project_id),
            'import_id': str(self.import_id),
            'query': self.query,
            'def_type': self.def_type,
            'field': self.field,
            'op': self.op
        }
