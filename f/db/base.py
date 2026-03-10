import json
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict
from sqlalchemy import JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator


class Base(DeclarativeBase):
    pass


class JSONModel(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")


class JSONData(TypeDecorator[Any]):
    impl: ClassVar[Any] = JSON
    model: type["JSONModel"]

    def __init__(self, model: type["JSONModel"], *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.model = model

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is not None:
            return value.model_dump()
        return value

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is not None:
            if isinstance(value, str):
                value = json.loads(value)
            value = self.model.model_validate(value)
        return value


class Translated(TypeDecorator[Any]):
    impl: ClassVar[Any] = JSON

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        if value is not None:
            return json.dumps(value)
        return value

    def process_result_value(self, value: Any, dialect: Any) -> Any:
        if value is not None:
            if not isinstance(value, dict):
                value = json.loads(value)
        return value
