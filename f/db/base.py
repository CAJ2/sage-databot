from sqlalchemy import JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeDecorator
import json
from dataclasses import asdict


class Base(DeclarativeBase):
    pass


class JSONData(TypeDecorator):
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
            value = json.loads(value)
            value = self.dataclass(**value)
        return value
