from typing import Any, ClassVar
from abc import ABC, abstractmethod

from pydantic import BaseModel


class TaskParams(BaseModel, ABC):
    func_name: ClassVar[str]

    @property
    @abstractmethod
    def payload(self) -> dict[str, Any]:
        ...
