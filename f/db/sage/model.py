from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from f.db.base import Base, JSONData, Translated


@dataclass
class SourceContent:
    text: str | None = None
    context: str | None = None
    icon: str | None = None


class Variant(Base):
    __tablename__ = "variants"

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[dict[str, str]] = mapped_column(Translated())
    desc: Mapped[dict[str, str]] = mapped_column(Translated())
    code: Mapped[str] = mapped_column()

    sources: Mapped[list["VariantSources"]] = relationship(
        "VariantSources", back_populates="variant", lazy="joined"
    )


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[str] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column()
    processed_at: Mapped[datetime | None] = mapped_column()
    location: Mapped[str | None] = mapped_column()
    content: Mapped[SourceContent | None] = mapped_column(JSONData(SourceContent))
    content_url: Mapped[str | None] = mapped_column()
    user_id: Mapped[str | None] = mapped_column()

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
    org_id: Mapped[str | None] = mapped_column()
    variant_id: Mapped[str | None] = mapped_column()
    component_id: Mapped[str | None] = mapped_column()
    process_id: Mapped[str | None] = mapped_column()


class Change(Base):
    __tablename__ = "changes"
    __table_args__ = {"schema": "public"}

    id: Mapped[str] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column()
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    updated_at: Mapped[datetime] = mapped_column()
