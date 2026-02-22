from dataclasses import dataclass
from typing import TypedDict
from sqlalchemy.orm import Mapped, mapped_column

from f.db.base import Base, JSONData


@dataclass
class CitiesTags:
    cities_tags: list[str]


@dataclass
class CountriesTags:
    countries_tags: list[str]


@dataclass
class DataSourcesTags:
    data_sources_tags: list[str]


@dataclass
class EcoscoreData:
    status: str | None = None
    score: int | None = None
    grade: str | None = None
    adjustments: dict | None = None


@dataclass
class GenericName:
    generic_name: list[dict]


class Image(TypedDict):
    key: str
    imgid: int | None
    rev: int | None
    sizes: dict[str, dict[str, str]]
    uploaded_t: int | None
    uploader: str | None


@dataclass
class Images:
    images: list[Image]


@dataclass
class Packagings:
    packagings: list[dict]


@dataclass
class ProductName:
    product_name: list[dict]


class OFFProduct(Base):
    __tablename__ = "off_products"

    id: Mapped[str] = mapped_column(primary_key=True)
    brands: Mapped[str | None]
    categories: Mapped[str | None]
    cities_tags: Mapped[CitiesTags | None] = mapped_column(JSONData(CitiesTags))
    countries_tags: Mapped[CountriesTags | None] = mapped_column(
        JSONData(CountriesTags)
    )
    data_sources_tags: Mapped[DataSourcesTags | None] = mapped_column(
        JSONData(DataSourcesTags)
    )
    ecoscore_data: Mapped[EcoscoreData | None] = mapped_column(JSONData(EcoscoreData))
    emb_codes: Mapped[str | None]
    generic_name: Mapped[GenericName | None] = mapped_column(JSONData(GenericName))
    images: Mapped[Images | None] = mapped_column(JSONData(Images))
    labels: Mapped[str | None]
    lang: Mapped[str | None]
    link: Mapped[str | None]
    manufacturing_places: Mapped[str | None]
    origins: Mapped[str | None]
    packagings: Mapped[Packagings | None] = mapped_column(JSONData(Packagings))
    product_name: Mapped[ProductName | None] = mapped_column(JSONData(ProductName))
    product_quantity: Mapped[str | None]
    product_quantity_unit: Mapped[str | None]
    stores: Mapped[str | None]
