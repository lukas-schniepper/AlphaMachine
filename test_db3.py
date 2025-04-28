# test_db.py (oder wie Dein Testskript heißt)

from AlphaMachine_core.db import init_db, engine
from AlphaMachine_core.data_manager import StockDataManager
from sqlalchemy import text, select
import pandas as pd
from datetime import date

# 1) Tabellen anlegen
init_db()

# 2) DataManager initialisieren
dm = StockDataManager()

# 3) Perioden-Eintrag mit echtem Ticker (AAPL) anlegen
next_month = date.today().replace(day=1)
start = next_month.strftime("%Y-%m-%d")
end   = (pd.to_datetime(start) + pd.offsets.MonthEnd(1)).strftime("%Y-%m-%d")
added = dm.add_tickers_for_period(["AAPL"], start, end, source_name="unittest")
print("▶️ Hinzugefügt:", added)

# 4) Delta-Load auslösen
updated = dm.update_ticker_data(tickers=["AAPL"])
print("▶️ Updated tickers:", updated)

# 5) Roh-Daten aus PriceData ansehen
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT ticker, date, close FROM pricedata WHERE ticker='AAPL' ORDER BY date DESC LIMIT 5"
    )).all()
    print("▶️ Raw PriceData rows:", rows)
