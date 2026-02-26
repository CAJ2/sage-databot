from datetime import datetime
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from f.db.base import Base, JSONData, Translated


class Variant(Base):
    __tablename__ = "variants"

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[dict[str, str]] = mapped_column(Translated())
    desc: Mapped[dict[str, str]] = mapped_column(Translated())
    code: Mapped[str]

    sources: Mapped[list["VariantSources"]] = relationship(
        "VariantSources", back_populates="variant", lazy="joined"
    )


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[str] = mapped_column(primary_key=True)
    type: Mapped[str]
    processed_at: Mapped[datetime | None]
    location: Mapped[str | None]
    content: Mapped[dict | None] = mapped_column(JSONData(dict))
    content_url: Mapped[str | None]
    user_id: Mapped[str | None]

    variants: Mapped[list["VariantSources"]] = relationship(
        "VariantSources", back_populates="source", lazy="joined"
    )


class VariantSources(Base):
    __tablename__ = "variants_sources"

    variant_id: Mapped[str] = mapped_column(ForeignKey("variants.id"), primary_key=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.id"), primary_key=True)

    variant: Mapped["Variant"] = relationship(
        "Variant", back_populates="sources", lazy="joined"
    )
    source: Mapped["Source"] = relationship(
        "Source", back_populates="variants", lazy="joined"
    )


class ExternalSource(Base):
    __tablename__ = "external_sources"

    source: Mapped[str] = mapped_column(primary_key=True)
    source_id: Mapped[str] = mapped_column(primary_key=True)
    org_id: Mapped[str | None]
    variant_id: Mapped[str | None]
    component_id: Mapped[str | None]
    process_id: Mapped[str | None]


class Change(Base):
    __tablename__ = "changes"
    __table_args__ = {"schema": "public"}

    id: Mapped[str] = mapped_column(primary_key=True)
    status: Mapped[str]
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB)
    updated_at: Mapped[datetime]
