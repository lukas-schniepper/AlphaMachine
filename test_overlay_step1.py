# In einem Test-Notebook oder kleinen Skript
from AlphaMachine_core.data_manager import StockDataManager
import streamlit as st
from AlphaMachine_core.db import get_session
from AlphaMachine_core.config import DATABASE_URL 
from AlphaMachine_core.data_manager import StockDataManager
sdm = StockDataManager()
test_tickers = ["SPY", "VIX", "AAII"] # Passe AAII an deinen echten Ticker an
data = sdm.get_price_data(tickers=test_tickers, start_date="2020-01-01", end_date="2024-01-01")
print(f"Anzahl PriceData-Objekte für {test_tickers}: {len(data)}")
for item in data[:5]: # Zeige die ersten paar Objekte
    print(item)