# AlphaMachine_core/models.py

from sqlmodel import SQLModel, Field
from datetime import date
from typing import Optional

class TickerPeriod(SQLModel, table=True):
    id: int        = Field(default=None, primary_key=True)
    ticker: str
    start_date: date
    end_date:   date
    source:     str

class TickerInfo(SQLModel, table=True):
    id: int               = Field(default=None, primary_key=True)
    ticker: str
    sector: str
    industry: Optional[str] = None
    currency: str
    country: Optional[str]  = None
    exchange: Optional[str] = None
    quote_type: Optional[str] = None
    market_cap: Optional[float] = None
    employees: Optional[int]  = None
    website: Optional[str]    = None
    actual_start_date: date
    actual_end_date:   date
    last_update:       date

class PriceData(SQLModel, table=True):
    id: int        = Field(default=None, primary_key=True)
    ticker: str
    date:   date
    close:  float
