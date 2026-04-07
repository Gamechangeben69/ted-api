"""
OCDS-inspiriertes Datenbankschema für TED IT-Ausschreibungen
=============================================================
Tabellen:
  buyers      – Auftraggeber (contracting authorities)
  tenders     – Ausschreibungen (contract notices, TD=3)
  lots        – Lose einer Ausschreibung
  awards      – Zuschläge (award notices, TD=7)
  suppliers   – Zuschlagsempfänger / Bieter
  rate_limits – Rate-Limiting für API-Keys
"""

import os
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

from sqlalchemy import (
    BigInteger, Column, Date, DateTime, Float, ForeignKey,
    Integer, String, Text, Index, create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

# ── DB-Verbindung ─────────────────────────────────────────────────────────────

_raw_url = os.environ.get("DATABASE_URL", "")
if _raw_url.startswith("postgres://"):
    _raw_url = _raw_url.replace("postgres://", "postgresql://", 1)

# Fallback: DB im Home-Verzeichnis speichern (funktioniert auf Windows + Linux)
_default_db = os.path.join(os.path.expanduser("~"), "ted_api_data", "ted_tenders.db")
os.makedirs(os.path.dirname(_default_db), exist_ok=True)
DATABASE_URL = _raw_url or f"sqlite:///{_default_db}"

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine       = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


# ── Modelle ───────────────────────────────────────────────────────────────────

class Buyer(Base):
    """Auftraggeber (Contracting Authority)"""
    __tablename__ = "buyers"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    name          = Column(String(500), nullable=False)
    nuts_code     = Column(String(10))    # z. B. DE212
    country_code  = Column(String(3))     # z. B. DEU
    created_at    = Column(DateTime, default=datetime.utcnow)

    tenders       = relationship("Tender", back_populates="buyer")

    def __repr__(self):
        return f"<Buyer {self.id}: {self.name[:40]}>"


class Tender(Base):
    """
    Ausschreibung (Contract Notice / Auftragsbekanntmachung, TD=3).
    Entspricht dem OCDS `tender`-Objekt plus Metadaten.
    """
    __tablename__ = "tenders"

    # Primärschlüssel = TED Dokumentnummer (z. B. 123456-2024)
    id                = Column(String(50), primary_key=True)

    # Verknüpfung
    buyer_id          = Column(Integer, ForeignKey("buyers.id"), nullable=True)
    buyer             = relationship("Buyer", back_populates="tenders")

    # TED-Metadaten
    ted_url           = Column(String(300))
    doc_type          = Column(String(30))   # z. B. "3", "cn", "can-social"
    doc_type_label    = Column(String(100))  # z. B. "Contract Notice"
    published_date    = Column(Date, index=True)
    deadline_date     = Column(Date, index=True, nullable=True)

    # Geografisch
    country_code      = Column(String(3), index=True)  # DEU
    nuts_code         = Column(String(10))              # DE212
    nuts_label        = Column(String(200))             # Bayern – München
    country_label     = Column(String(100))             # Germany

    # Inhalt
    title             = Column(Text)
    description       = Column(Text, nullable=True)

    # CPV
    cpv_codes         = Column(Text)                    # JSON-Array ["72263000"]
    cpv_main          = Column(String(10))              # Haupt-CPV z. B. "72263000"
    cpv_main_label    = Column(String(200))             # "Softwarepflege-Dienstleistungen"
    cpv_category      = Column(String(20))              # "72" (IT Services)
    cpv_category_label= Column(String(100))             # "IT Services & Consulting"

    # Verfahren & Typ
    contract_type         = Column(String(10))
    contract_type_label   = Column(String(100))
    procedure             = Column(String(10))
    procedure_label       = Column(String(100))
    award_criteria        = Column(String(10))
    award_criteria_label  = Column(String(100))

    # Verknüpfung zu Award Notice
    award_notice_id   = Column(String(50), nullable=True)  # TD=7 Dokument-Nr.

    # Timestamps
    scraped_at        = Column(DateTime, default=datetime.utcnow)
    updated_at        = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    lots              = relationship("Lot",   back_populates="tender", cascade="all, delete-orphan")
    awards            = relationship("Award", back_populates="tender", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_country_pub",  "country_code", "published_date"),
        Index("ix_country_dead", "country_code", "deadline_date"),
    )

    def __repr__(self):
        return f"<Tender {self.id}>"


class Lot(Base):
    """
    Los einer Ausschreibung.
    OCDS: `tender.lots[]`
    """
    __tablename__ = "lots"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    tender_id     = Column(String(50), ForeignKey("tenders.id"), nullable=False, index=True)
    tender        = relationship("Tender", back_populates="lots")

    lot_number    = Column(Integer, nullable=True)
    title         = Column(Text,    nullable=True)
    description   = Column(Text,    nullable=True)
    cpv_codes     = Column(Text,    nullable=True)    # JSON-Array
    estimated_value    = Column(Float, nullable=True)
    estimated_currency = Column(String(3), nullable=True)  # EUR
    deadline_date = Column(Date,    nullable=True)

    created_at    = Column(DateTime, default=datetime.utcnow)

    awards        = relationship("Award", back_populates="lot")

    def __repr__(self):
        return f"<Lot {self.id} (tender={self.tender_id}, nr={self.lot_number})>"


class Supplier(Base):
    """
    Bieter / Zuschlagsempfänger.
    OCDS: `awards[].suppliers[]`
    """
    __tablename__ = "suppliers"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    name          = Column(String(500), nullable=False)
    nuts_code     = Column(String(10),  nullable=True)
    country_code  = Column(String(3),   nullable=True)
    created_at    = Column(DateTime, default=datetime.utcnow)

    awards        = relationship("Award", back_populates="supplier")

    def __repr__(self):
        return f"<Supplier {self.id}: {self.name[:40]}>"


class Award(Base):
    """
    Zuschlag / Award Notice (TD=7).
    OCDS: `awards[]`
    Verknüpft ein Tender mit einem Supplier (ggf. lot-spezifisch).
    """
    __tablename__ = "awards"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    tender_id     = Column(String(50), ForeignKey("tenders.id"),   nullable=False, index=True)
    lot_id        = Column(Integer,    ForeignKey("lots.id"),       nullable=True)
    supplier_id   = Column(Integer,    ForeignKey("suppliers.id"),  nullable=True)

    tender        = relationship("Tender",   back_populates="awards")
    lot           = relationship("Lot",      back_populates="awards")
    supplier      = relationship("Supplier", back_populates="awards")

    # TED-Metadaten des Award Notice
    award_notice_id   = Column(String(50), nullable=True)
    award_date        = Column(Date,       nullable=True)
    published_date    = Column(Date,       nullable=True)

    # Vertragswert
    contract_value    = Column(Float,   nullable=True)
    contract_currency = Column(String(3), nullable=True)  # EUR
    offers_received   = Column(Integer, nullable=True)

    created_at        = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Award {self.id} tender={self.tender_id} val={self.contract_value}>"


class RateLimit(Base):
    """Rate-Limiting für API-Keys (daily window)."""
    __tablename__ = "rate_limits"

    api_key      = Column(String(200), primary_key=True)
    tier         = Column(String(20),  nullable=False, default="free")
    req_count    = Column(Integer,     nullable=False, default=0)
    window_start = Column(DateTime,    nullable=False, default=datetime.utcnow)


class Alert(Base):
    """
    Gespeichertes Suchprofil für Deadline-Alerting.
    Nutzer legen Suchkriterien fest; der tägl. Job findet neue Matches.
    """
    __tablename__ = "alerts"

    id           = Column(Integer,     primary_key=True, autoincrement=True)
    name         = Column(String(200), nullable=False)
    email        = Column(String(200), nullable=True)   # Empfänger (optional)
    webhook_url  = Column(String(500), nullable=True)   # Webhook (optional)

    # Suchkriterien (alle optional, AND-verknüpft)
    keyword      = Column(String(500), nullable=True)
    country      = Column(String(10),  nullable=True)   # DEU, FRA, …
    cpv_prefix   = Column(String(10),  nullable=True)   # 72, 48, …
    min_value    = Column(Float,       nullable=True)
    doc_type     = Column(String(20),  nullable=True)   # cn / can

    # Status
    active       = Column(Integer,     nullable=False, default=1)   # 1=aktiv, 0=pausiert
    last_run     = Column(DateTime,    nullable=True)
    last_matches = Column(Integer,     nullable=True, default=0)

    created_at   = Column(DateTime,    default=datetime.utcnow)
    updated_at   = Column(DateTime,    default=datetime.utcnow, onupdate=datetime.utcnow)


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
