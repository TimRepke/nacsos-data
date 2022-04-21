from pydantic import BaseModel, validator
from typing import Literal, Optional, Union, TypeVar, ForwardRef, Tuple
from sqlmodel import SQLModel, Field, Relationship
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Column


class AnnotationTaskLabel(BaseModel):
    name: str
    key: str
    hint: Optional[str]
    max_repeat: int = 1
    required: bool = True
    kind: Literal['bool', 'single', 'multi', 'str', 'int', 'float'] = 'single'
    choices: Optional[list['AnnotationTaskLabelChoice']]


class AnnotationTaskLabelChoice(BaseModel):
    name: str
    hint: Optional[str]
    value: int
    child: Optional['AnnotationTaskLabel']


class AnnotationTask(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: Optional[int] = Field(default=None, foreign_key='user.id')
    project_id: Optional[int] = Field(default=None, foreign_key='project.id')
    name: str
    description: str
    labels: list['AnnotationTaskLabel'] = Field(sa_column=Column(JSONB))


class Assignment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: Optional[int] = Field(default=None, foreign_key='user.id')
    item_id: int
    task_id: Optional[int] = Field(default=None, foreign_key='annotation_task.id')


class Annotation(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now)
    user_id: Optional[int] = Field(default=None, foreign_key='user.id')
    item_id: Optional[int] = Field(default=None, foreign_key='item.id')
    task_id: Optional[int] = Field(default=None, foreign_key='annotation_task.id')
    assignment_id: Optional[int] = Field(default=None, foreign_key='assignment.id')
    key: str
    repeat: int

    value_bool: Optional[bool]
    value_int: Optional[int]
    value_float: Optional[float]
    value_str: Optional[str]

    text_offset_start: Optional[int]
    text_offset_stop: Optional[int]
