from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from f.db.base import Base, JSONData, JSONModel, Translated


class SourcePromptResult(JSONModel):
    id: str
    prompt_id: str
    model: str
    output: dict[str, Any]
    created_at: str


class SourceContent(JSONModel):
    text: str | None = None
    context: str | None = None
    icon: str | None = None
    prompts: list[SourcePromptResult] | None = None


class Variant(Base):
    __tablename__: str = "variants"

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[dict[str, str]] = mapped_column(Translated())
    desc: Mapped[dict[str, str]] = mapped_column(Translated())
    code: Mapped[str] = mapped_column()

    sources: Mapped[list["VariantSources"]] = relationship(
        "VariantSources", back_populates="variant", lazy="joined"
    )


class Source(Base):
    __tablename__: str = "sources"

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
    component_sources: Mapped[list["ComponentSources"]] = relationship(
        "ComponentSources", back_populates="source", lazy="joined"
    )
    process_sources: Mapped[list["ProcessSources"]] = relationship(
        "ProcessSources", back_populates="source", lazy="joined"
    )


class VariantSources(Base):
    __tablename__: str = "variants_sources"

    variant_id: Mapped[str] = mapped_column(ForeignKey("variants.id"), primary_key=True)
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.id"), primary_key=True)

    variant: Mapped["Variant"] = relationship(
        "Variant", back_populates="sources", lazy="joined"
    )
    source: Mapped["Source"] = relationship(
        "Source", back_populates="variants", lazy="joined"
    )


class ComponentSources(Base):
    __tablename__: str = "components_sources"

    component_id: Mapped[str] = mapped_column(
        ForeignKey("components.id"), primary_key=True
    )
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.id"), primary_key=True)

    source: Mapped["Source"] = relationship(
        "Source", back_populates="component_sources", lazy="joined"
    )


class ProcessSources(Base):
    __tablename__: str = "process_sources"

    process_id: Mapped[str] = mapped_column(
        ForeignKey("processes.id"), primary_key=True
    )
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.id"), primary_key=True)

    source: Mapped["Source"] = relationship(
        "Source", back_populates="process_sources", lazy="joined"
    )


class ExternalSource(Base):
    __tablename__: str = "external_sources"

    source: Mapped[str] = mapped_column(primary_key=True)
    source_id: Mapped[str] = mapped_column(primary_key=True)
    org_id: Mapped[str | None] = mapped_column()
    variant_id: Mapped[str | None] = mapped_column()
    component_id: Mapped[str | None] = mapped_column()
    process_id: Mapped[str | None] = mapped_column()


class Change(Base):
    __tablename__: str = "changes"
    __table_args__: dict[str, str] = {"schema": "public"}

    id: Mapped[str] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column()
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    updated_at: Mapped[datetime] = mapped_column()
