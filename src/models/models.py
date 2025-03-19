from datetime import datetime
from typing import Optional, List
from sqlalchemy import String, DateTime, Boolean, ForeignKey, JSON, Enum, Float, Integer, create_engine
from sqlalchemy.orm import sessionmaker, relationship, DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
import uuid
import enum
import os
from dotenv import load_dotenv
from datetime import timezone

# Load environment variables from .env file
load_dotenv()

# Database connection string for CockroachDB
DATABASE_URL = os.getenv('DATABASE_URL')

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a session
session = Session()

# Create declarative base
class Base(DeclarativeBase):
    pass

class ContentType(enum.Enum):
    """Enum for different types of content that can be processed"""
    HTML = "html"
    PDF = "pdf"
    IMAGE = "image"
    OTHER = "other"

class ProcessingStatus(enum.Enum):
    """Enum for tracking processing status of content"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class CrawlTarget(Base):
    """
    Stores the initial URLs to crawl and their crawling metadata.
    This table represents the root URLs that should be periodically crawled.
    """
    __tablename__ = 'crawl_targets'

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    base_url: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    spider: Mapped[str] = mapped_column(String, nullable=False, default="standard")
    settings: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    crawl_frequency_hours: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_crawl_started: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_crawl_completed: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    # Relationships
    crawled_pages: Mapped[List["CrawledPage"]] = relationship("CrawledPage", back_populates="crawl_target")

class CrawledPage(Base):
    """
    Stores information about individual pages that have been crawled,
    including their content and extraction status.
    """
    __tablename__ = 'crawled_pages'

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    crawl_target_id: Mapped[uuid.UUID] = mapped_column(PG_UUID, ForeignKey('crawl_targets.id'), nullable=False)
    url: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    html_archive_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_crawled: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    has_extractable_content: Mapped[bool] = mapped_column(Boolean, default=False)
    extraction_status: Mapped[ProcessingStatus] = mapped_column(Enum(ProcessingStatus), default=ProcessingStatus.PENDING)
    page_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    meta_description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    spacy_analysis: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    # Relationships
    crawl_target: Mapped["CrawlTarget"] = relationship("CrawlTarget", back_populates="crawled_pages")
    extracted_data: Mapped[List["ExtractedData"]] = relationship("ExtractedData", back_populates="source_page")

class Document(Base):
    """
    Stores information about additional documents (PDFs, images, etc.)
    that may contain extractable information.
    """
    __tablename__ = 'documents'

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    source_url: Mapped[str] = mapped_column(String, nullable=False)
    file_path: Mapped[str] = mapped_column(String, nullable=False)
    content_type: Mapped[ContentType] = mapped_column(Enum(ContentType), nullable=False)
    file_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    extraction_status: Mapped[ProcessingStatus] = mapped_column(Enum(ProcessingStatus), default=ProcessingStatus.PENDING)
    processing_metadata: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    ocr_required: Mapped[bool] = mapped_column(Boolean, default=False)
    ocr_completed: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    # Relationships
    extracted_data: Mapped[List["ExtractedData"]] = relationship("ExtractedData", back_populates="source_document")

class ExtractedData(Base):
    """
    Stores structured data extracted from crawled pages and documents,
    along with metadata about the extraction process.
    """
    __tablename__ = 'extracted_data'

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    source_page_id: Mapped[Optional[uuid.UUID]] = mapped_column(PG_UUID, ForeignKey('crawled_pages.id'), nullable=True)
    source_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(PG_UUID, ForeignKey('documents.id'), nullable=True)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    extraction_method: Mapped[str] = mapped_column(String, nullable=False)
    confidence_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    extraction_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    validation_status: Mapped[ProcessingStatus] = mapped_column(Enum(ProcessingStatus), default=ProcessingStatus.PENDING)
    validation_notes: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    version: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))

    # Relationships
    source_page: Mapped[Optional["CrawledPage"]] = relationship("CrawledPage", back_populates="extracted_data")
    source_document: Mapped[Optional["Document"]] = relationship("Document", back_populates="extracted_data")

# Create all tables
def init_db():
    Base.metadata.create_all(engine)