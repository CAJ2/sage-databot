import inspect
from typing import Any
from sqlalchemy import JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator
import json
from dataclasses import asdict


class Base(DeclarativeBase):
    pass


class JSONData(TypeDecorator[Any]):
    impl = JSON

    def __init__(self, dataclass, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataclass = dataclass

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(asdict(value))
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            if not isinstance(value, dict):
                value = json.loads(value)
            value = self.dataclass(
                **{
                    k: v
                    for k, v in value.items()
                    if k in inspect.signature(self.dataclass).parameters
                }
            )
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
