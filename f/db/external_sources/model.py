from sqlalchemy.orm import Mapped, mapped_column

from f.db.base import Base


class ExternalSource(Base):
    __tablename__ = "external_sources"

    source: Mapped[str] = mapped_column(primary_key=True)
    source_id: Mapped[str] = mapped_column(primary_key=True)
    org_id: Mapped[str | None]
    variant_id: Mapped[str | None]
    component_id: Mapped[str | None]
    process_id: Mapped[str | None]
