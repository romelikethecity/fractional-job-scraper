"""
Database models for fractional job scraper.
Uses SQLAlchemy for ORM with SQLite (dev) or PostgreSQL (prod).
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean, 
    DateTime, Text, Enum, ForeignKey, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import enum

Base = declarative_base()


class Source(enum.Enum):
    INDEED = "indeed"
    FRACTIONALJOBS = "fractionaljobs"
    FREEAGENT = "freeagent"
    SHINY = "shiny"
    GIGX = "gigx"


class CompensationType(enum.Enum):
    HOURLY = "hourly"
    MONTHLY = "monthly"
    ANNUAL = "annual"
    EQUITY_ONLY = "equity_only"
    NOT_DISCLOSED = "not_disclosed"


class LocationType(enum.Enum):
    REMOTE = "remote"
    HYBRID = "hybrid"
    ONSITE = "onsite"


class LocationRestriction(enum.Enum):
    WORLDWIDE = "worldwide"
    USA_ONLY = "usa_only"
    STATE_SPECIFIC = "state_specific"
    TIMEZONE = "timezone"
    CITY_SPECIFIC = "city_specific"


class FunctionCategory(enum.Enum):
    FINANCE = "finance"
    MARKETING = "marketing"
    SALES = "sales"
    PRODUCT = "product"
    ENGINEERING = "engineering"
    OPERATIONS = "operations"
    PEOPLE = "people"
    DATA = "data"
    LEGAL = "legal"
    OTHER = "other"


class SeniorityTier(enum.Enum):
    C_LEVEL = "c_level"
    EVP = "evp"
    SVP = "svp"
    VP = "vp"
    DIRECTOR = "director"
    HEAD_OF = "head_of"
    UNKNOWN = "unknown"


class FundingStage(enum.Enum):
    SEED = "seed"
    SERIES_A = "series_a"
    SERIES_B = "series_b"
    SERIES_C = "series_c"
    GROWTH = "growth"
    PUBLIC = "public"
    PE_BACKED = "pe_backed"
    BOOTSTRAPPED = "bootstrapped"
    UNKNOWN = "unknown"


class FractionalJob(Base):
    """Core job listing table."""
    
    __tablename__ = "fractional_jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Source tracking
    source = Column(String(50), nullable=False, index=True)
    source_id = Column(String(255), nullable=False)  # Unique ID from source
    source_url = Column(Text)
    
    # Core job info
    title = Column(String(500), nullable=False)
    company_name = Column(String(255), index=True)
    company_url = Column(Text)
    
    # Location
    location_raw = Column(String(255))
    location_type = Column(String(50))  # remote/hybrid/onsite
    location_restriction = Column(String(50))  # worldwide/usa_only/state_specific/timezone
    location_state = Column(String(50))  # If state-specific
    location_city = Column(String(100))  # If city-specific
    
    # Compensation (raw from source)
    compensation_type = Column(String(50))  # hourly/monthly/annual/equity_only/not_disclosed
    compensation_min = Column(Float)
    compensation_max = Column(Float)
    compensation_currency = Column(String(10), default="USD")
    
    # Compensation (normalized)
    hourly_rate_min = Column(Float)
    hourly_rate_max = Column(Float)
    monthly_retainer_min = Column(Float)
    monthly_retainer_max = Column(Float)
    
    # Hours commitment
    hours_per_week_min = Column(Float)
    hours_per_week_max = Column(Float)
    
    # Job classification
    job_type = Column(String(50))  # contract/part_time/full_time
    experience_level = Column(String(50))  # entry/mid/senior/executive
    function_category = Column(String(50), index=True)
    seniority_tier = Column(String(50), index=True)
    
    # Dates
    date_posted = Column(DateTime)
    date_scraped = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    
    # Content
    description_raw = Column(Text)
    description_snippet = Column(Text)  # First 500 chars for previews
    benefits_raw = Column(Text)
    requirements_raw = Column(Text)
    
    # Status
    is_active = Column(Boolean, default=True, index=True)
    easy_apply = Column(Boolean)
    
    # Relationships
    company_id = Column(Integer, ForeignKey("company_enrichment.id"))
    company = relationship("CompanyEnrichment", back_populates="jobs")
    
    __table_args__ = (
        Index("idx_source_source_id", "source", "source_id", unique=True),
        Index("idx_active_function", "is_active", "function_category"),
        Index("idx_active_seniority", "is_active", "seniority_tier"),
        Index("idx_date_posted", "date_posted"),
    )
    
    def __repr__(self):
        return f"<FractionalJob(id={self.id}, title='{self.title}', company='{self.company_name}')>"
    
    @property
    def compensation_display(self) -> str:
        """Human-readable compensation string."""
        if self.compensation_type == "not_disclosed":
            return "Not disclosed"
        elif self.compensation_type == "hourly":
            if self.compensation_min == self.compensation_max:
                return f"${self.compensation_min:.0f}/hr"
            return f"${self.compensation_min:.0f}-${self.compensation_max:.0f}/hr"
        elif self.compensation_type == "monthly":
            if self.compensation_min == self.compensation_max:
                return f"${self.compensation_min:,.0f}/mo"
            return f"${self.compensation_min:,.0f}-${self.compensation_max:,.0f}/mo"
        elif self.compensation_type == "annual":
            if self.compensation_min == self.compensation_max:
                return f"${self.compensation_min:,.0f}/yr"
            return f"${self.compensation_min:,.0f}-${self.compensation_max:,.0f}/yr"
        return "Unknown"
    
    @property
    def hours_display(self) -> str:
        """Human-readable hours string."""
        if not self.hours_per_week_min:
            return "Not specified"
        if self.hours_per_week_min == self.hours_per_week_max:
            return f"{self.hours_per_week_min:.0f} hrs/week"
        return f"{self.hours_per_week_min:.0f}-{self.hours_per_week_max:.0f} hrs/week"


class CompanyEnrichment(Base):
    """Enriched company data from Crunchbase/PitchBook."""
    
    __tablename__ = "company_enrichment"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Identifiers
    company_name = Column(String(255), nullable=False, index=True)
    company_name_normalized = Column(String(255), index=True)  # Lowercase, stripped
    company_url = Column(Text)
    crunchbase_url = Column(Text)
    pitchbook_url = Column(Text)
    linkedin_url = Column(Text)
    
    # Funding
    funding_stage = Column(String(50))
    total_funding = Column(Float)  # In USD
    last_funding_date = Column(DateTime)
    last_funding_amount = Column(Float)
    investors = Column(Text)  # JSON array of investor names
    
    # Company metrics
    employee_count = Column(Integer)
    employee_count_range = Column(String(50))  # "11-50", "51-200", etc.
    revenue_range = Column(String(50))  # "$1M-$10M", etc.
    founded_year = Column(Integer)
    
    # Classification
    industry = Column(String(100))
    sub_industry = Column(String(100))
    hq_location = Column(String(255))
    hq_country = Column(String(100))
    
    # Metadata
    date_enriched = Column(DateTime, default=datetime.utcnow)
    enrichment_source = Column(String(50))  # crunchbase/pitchbook/manual
    enrichment_confidence = Column(Float)  # 0-1 confidence score
    
    # Relationships
    jobs = relationship("FractionalJob", back_populates="company")
    
    __table_args__ = (
        Index("idx_company_name_normalized", "company_name_normalized"),
    )
    
    def __repr__(self):
        return f"<CompanyEnrichment(id={self.id}, name='{self.company_name}')>"


class CompensationSnapshot(Base):
    """Weekly aggregated compensation data for trend analysis."""
    
    __tablename__ = "compensation_snapshots"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Snapshot metadata
    snapshot_date = Column(DateTime, nullable=False, index=True)
    
    # Dimensions
    function_category = Column(String(50))
    seniority_tier = Column(String(50))
    location_type = Column(String(50))
    hours_bucket = Column(String(50))  # "5-10", "10-20", "20-30", "30-40"
    
    # Sample info
    sample_size = Column(Integer)
    
    # Hourly rate stats
    hourly_rate_min_avg = Column(Float)
    hourly_rate_max_avg = Column(Float)
    hourly_rate_median = Column(Float)
    hourly_rate_p25 = Column(Float)
    hourly_rate_p75 = Column(Float)
    
    # Monthly retainer stats
    monthly_retainer_min_avg = Column(Float)
    monthly_retainer_max_avg = Column(Float)
    monthly_retainer_median = Column(Float)
    
    __table_args__ = (
        Index("idx_snapshot_dimensions", "snapshot_date", "function_category", "seniority_tier"),
    )


class ListingSnapshot(Base):
    """Daily count of active listings for trend charts."""
    
    __tablename__ = "listing_snapshots"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    snapshot_date = Column(DateTime, nullable=False, index=True)
    source = Column(String(50))
    
    # Counts
    total_active = Column(Integer)
    new_today = Column(Integer)
    removed_today = Column(Integer)
    
    # Breakdowns
    by_function = Column(Text)  # JSON: {"finance": 45, "marketing": 32, ...}
    by_seniority = Column(Text)  # JSON
    by_location_type = Column(Text)  # JSON
    by_hours_bucket = Column(Text)  # JSON
    
    # Compensation transparency
    comp_disclosed_count = Column(Integer)
    comp_disclosed_pct = Column(Float)


class ScrapeLog(Base):
    """Logging table for scrape runs."""
    
    __tablename__ = "scrape_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    source = Column(String(50), nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    
    # Results
    status = Column(String(50))  # success/failed/partial
    listings_found = Column(Integer)
    listings_new = Column(Integer)
    listings_updated = Column(Integer)
    listings_deactivated = Column(Integer)
    
    # Errors
    error_message = Column(Text)
    error_count = Column(Integer, default=0)


# Database setup utilities

def create_database(db_url: str = "sqlite:///fractional_jobs.db"):
    """Create database and all tables."""
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    return engine


def get_session(engine):
    """Get a database session."""
    Session = sessionmaker(bind=engine)
    return Session()


if __name__ == "__main__":
    # Create SQLite database for development
    engine = create_database()
    print("Database created successfully!")
    print(f"Tables: {Base.metadata.tables.keys()}")
