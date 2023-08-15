import uuid
from typing import Any, Literal

from .abc import TaskParams


class WOSImport(TaskParams):
    func_name: Literal['nacsos_lib.academic.import.import_wos_file']  # type: ignore[misc]

    project_id: str | uuid.UUID
    import_id: str | uuid.UUID
    filenames: list[str]

    @property
    def payload(self) -> dict[str, Any]:
        return {
            'project_id': str(self.project_id),
            'import_id': str(self.import_id),
            'records': {
                'user_serializer': 'WebOfScienceSerializer',
                'user_dtype': 'AcademicItemModel',
                'filenames': self.filenames
            }
        }
