from inspect import Signature
from typing import Any, TYPE_CHECKING, ClassVar, Mapping, List, Optional, Dict, Type, Tuple, Callable
from pydantic import BaseModel, BaseConfig
from pydantic.fields import ModelField, ModelPrivateAttr
from pydantic.typing import AnyCallable, SetStr, DictAny


class SBaseModel(BaseModel):
    """Allows a BaseModel to return its fields by string variable indexing"""
    if TYPE_CHECKING:
        # populated by the metaclass, defined here to help IDEs only
        __fields__: ClassVar[Dict[str, ModelField]] = {}
        __include_fields__: ClassVar[Optional[Mapping[str, Any]]] = None
        __exclude_fields__: ClassVar[Optional[Mapping[str, Any]]] = None
        __validators__: ClassVar[Dict[str, AnyCallable]] = {}
        __pre_root_validators__: ClassVar[List[AnyCallable]]
        __post_root_validators__: ClassVar[List[Tuple[bool, AnyCallable]]]
        __config__: ClassVar[Type[BaseConfig]] = BaseConfig
        __json_encoder__: ClassVar[Callable[[Any], Any]] = lambda x: x
        __schema_cache__: ClassVar['DictAny'] = {}
        __custom_root_type__: ClassVar[bool] = False
        __signature__: ClassVar['Signature']
        __private_attributes__: ClassVar[Dict[str, ModelPrivateAttr]]
        __class_vars__: ClassVar[SetStr]
        __fields_set__: ClassVar[SetStr] = set()

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)
