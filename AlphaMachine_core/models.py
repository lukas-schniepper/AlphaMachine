from sqlmodel import SQLModel, Field
from datetime import date
from typing import Optional
from sqlalchemy import Column
from sqlalchemy.types import BigInteger

class TickerPeriod(SQLModel, table=True):
    id: int            = Field(default=None, primary_key=True)
    ticker: str
    start_date: date
    end_date:   date
    source:     str

class TickerInfo(SQLModel, table=True):
    id: int                      = Field(default=None, primary_key=True)
    ticker: str
    sector: Optional[str]        = None   # jetzt nullable
    industry: Optional[str]      = None
    currency: Optional[str]      = None   # jetzt nullable
    country: Optional[str]       = None
    exchange: Optional[str]      = None
    quote_type: Optional[str]    = None
    market_cap: Optional[float]  = None
    employees: Optional[int]     = None
    website: Optional[str]       = None
    actual_start_date: date
    actual_end_date:   date
    last_update:       date

class PriceData(SQLModel, table=True):
    id:     int     = Field(default=None, primary_key=True)
    ticker: str
    date:   date
    open:   float
    high:   float
    low:    float
    close:  float
    # Use BigInteger for volume, set nullability in Column
    volume: int     = Field(sa_column=Column(BigInteger, nullable=False))
