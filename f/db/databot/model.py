from datetime import datetime
from typing import Any, cast

from sqlalchemy import JSON, DateTime, Engine, Table
from sqlalchemy.orm import Mapped, mapped_column

from f.db.base import Base, JSONData, JSONModel


class CitiesTags(JSONModel):
    cities_tags: list[str]


class CountriesTags(JSONModel):
    countries_tags: list[str]


class DataSourcesTags(JSONModel):
    data_sources_tags: list[str]


class EcoscoreData(JSONModel):
    status: str | None = None
    score: int | None = None
    grade: str | None = None
    adjustments: dict[str, Any] | None = None


class GenericName(JSONModel):
    generic_name: list[dict[str, Any]]


class ImageSize(JSONModel):
    h: int
    w: int


class Image(JSONModel):
    key: str
    imgid: int | None = None
    rev: int | None = None
    sizes: dict[str, ImageSize | None] | None = None
    uploaded_t: int | None = None
    uploader: str | None = None


class Images(JSONModel):
    images: list[Image]


class Packagings(JSONModel):
    packagings: list[dict[str, Any]]


class ProductName(JSONModel):
    product_name: list[dict[str, Any]]


class OFFProduct(Base):
    __tablename__ = "off_products"

    id: Mapped[str] = mapped_column(primary_key=True)
    brands: Mapped[str | None] = mapped_column()
    categories: Mapped[str | None] = mapped_column()
    cities_tags: Mapped[CitiesTags | None] = mapped_column(JSONData(CitiesTags))
    countries_tags: Mapped[CountriesTags | None] = mapped_column(
        JSONData(CountriesTags)
    )
    data_sources_tags: Mapped[DataSourcesTags | None] = mapped_column(
        JSONData(DataSourcesTags)
    )
    ecoscore_data: Mapped[EcoscoreData | None] = mapped_column(JSONData(EcoscoreData))
    emb_codes: Mapped[str | None] = mapped_column()
    generic_name: Mapped[GenericName | None] = mapped_column(JSONData(GenericName))
    images: Mapped[Images | None] = mapped_column(JSONData(Images))
    labels: Mapped[str | None] = mapped_column()
    lang: Mapped[str | None] = mapped_column()
    link: Mapped[str | None] = mapped_column()
    manufacturing_places: Mapped[str | None] = mapped_column()
    origins: Mapped[str | None] = mapped_column()
    packagings: Mapped[Packagings | None] = mapped_column(JSONData(Packagings))
    product_name: Mapped[ProductName | None] = mapped_column(JSONData(ProductName))
    product_quantity: Mapped[str | None] = mapped_column()
    product_quantity_unit: Mapped[str | None] = mapped_column()
    stores: Mapped[str | None] = mapped_column()


class KGCache(Base):
    __tablename__ = "kg_cache"

    mid: Mapped[str] = mapped_column(primary_key=True)
    jsonld: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class WikidataCache(Base):
    __tablename__ = "wikidata_cache"

    qid: Mapped[str] = mapped_column(primary_key=True)
    jsonld: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


def ensure_cache_tables(engine: Engine) -> None:
    """Create KGCache and WikidataCache tables if they don't already exist."""
    for table in (KGCache.__table__, WikidataCache.__table__):
        cast(Table, table).create(engine, checkfirst=True)
