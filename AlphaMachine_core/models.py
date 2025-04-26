from sqlmodel import SQLModel, Field
from datetime import date

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
    currency: str
    actual_start_date: date
    actual_end_date:   date
    last_update:       date

class PriceData(SQLModel, table=True):
    id: int        = Field(default=None, primary_key=True)
    ticker: str
    date:   date
    close:  float
