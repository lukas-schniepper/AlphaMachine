# AlphaMachine_core/models.py

from typing import Optional
from datetime import date

from sqlmodel import SQLModel, Field
from sqlalchemy import Index, Column
from sqlalchemy.types import BigInteger

class TickerPeriod(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    # ticker darf jetzt mehrfach vorkommen, wird nicht mehr unique
    ticker: str = Field(index=True)
    start_date: date
    end_date: date
    source: str

    # Composite-Unique-Index verhindert nur Duplikate in derselben Periode+Quelle
    __table_args__ = (
        Index(
            "ux_tickerperiod_ticker_start_source",
            "ticker",
            "start_date",
            "source",
            unique=True
        ),
    )


class TickerInfo(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticker: str
    sector: Optional[str]        = None
    industry: Optional[str]      = None
    currency: Optional[str]      = None
    country: Optional[str]       = None
    exchange: Optional[str]      = None
    quote_type: Optional[str]    = None
    market_cap: Optional[float]  = None
    employees: Optional[int]     = None
    website: Optional[str]       = None
    actual_start_date: date
    actual_end_date: date
    last_update: date


class PriceData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    # volume als BigInteger, nicht-nullable
    volume: int = Field(sa_column=Column(BigInteger, nullable=False))
