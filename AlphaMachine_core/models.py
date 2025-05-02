# AlphaMachine_core/models.py
from __future__ import annotations
from typing           import Optional
from datetime         import date
from sqlmodel         import SQLModel, Field
from sqlalchemy       import Column, Date, UniqueConstraint
from sqlalchemy.types import BigInteger

class TickerPeriod(SQLModel, table=True):
    __tablename__  = "ticker_period"
    __table_args__ = (
      UniqueConstraint(
        "ticker", "start_date", "end_date", "source",
        name="uix_tickerperiod_ticker_start_end_source"
      ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    ticker: str       = Field(index=True, description="Ticker-Symbol")
    start_date: date  = Field(description="Perioden-Startdatum")
    end_date: date    = Field(description="Perioden-Enddatum")
    source: str       = Field(description="Datenquelle")


class TickerInfo(SQLModel, table=True):
    __tablename__ = "ticker_info"  # <-- falls dein DB-Schema so heißt; sonst weglassen
    id: Optional[int]         = Field(default=None, primary_key=True)
    ticker: str               = Field(index=True, description="Ticker-Symbol")
    sector: Optional[str]     = Field(default=None, description="Branche")
    industry: Optional[str]   = Field(default=None, description="Industrie")
    currency: Optional[str]   = Field(default=None, description="Währung")
    country: Optional[str]    = Field(default=None, description="Land")
    exchange: Optional[str]   = Field(default=None, description="Börse")
    quote_type: Optional[str] = Field(default=None, description="Art des Finanzinstruments")
    market_cap: Optional[float] = Field(default=None, description="Marktkapitalisierung")
    employees: Optional[int]    = Field(default=None, description="Anzahl Mitarbeiter")
    website: Optional[str]      = Field(default=None, description="Unternehmens-Website")
    actual_start_date: date     = Field(description="Erster verfügbarer Preistag")
    actual_end_date: date       = Field(description="Letzter verfügbarer Preistag")
    last_update: date           = Field(description="Datum der letzten Daten-Aktualisierung")


class PriceData(SQLModel, table=True):
    __tablename__ = "price_data"

    id: Optional[int] = Field(default=None, primary_key=True)
    ticker: str       = Field(index=True, description="Ticker-Symbol")

    # Python-Attribut trade_date → DB-Spalte "date"
    trade_date: date = Field(
        sa_column=Column("date", Date, index=True),
        description="Handelsdatum",
    )

    open:  float = Field(description="Eröffnungskurs")
    high:  float = Field(description="Höchstkurs")
    low:   float = Field(description="Tiefstkurs")
    close: float = Field(description="Schlusskurs")
    volume: int   = Field(
        sa_column=Column(BigInteger, nullable=False),
        description="Handelsvolumen"
    )
