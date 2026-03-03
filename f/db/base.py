import json
from typing import Any

from pydantic import BaseModel, ConfigDict
from sqlalchemy import JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator


class Base(DeclarativeBase):
    pass


class JSONModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class JSONData(TypeDecorator[Any]):
    impl = JSON

    def __init__(self, model: type[JSONModel], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = model

    def process_bind_param(self, value, dialect):
        if value is not None:
            return value.model_dump()
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = self.model.model_validate(value)
        return value


class Translated(TypeDecorator[Any]):
    impl = JSON

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            if not isinstance(value, dict):
                value = json.loads(value)
        return value
