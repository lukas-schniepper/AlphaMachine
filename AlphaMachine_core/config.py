# AlphaMachine_core/config.py
import os
from pathlib import Path
import streamlit as st # Muss importiert werden, um st.secrets zu verwenden

_THIS_DIR = Path(__file__).resolve().parent   

print("DEBUG [config.py]: Lese DATABASE_URL...")

# Versuche, DATABASE_URL aus st.secrets zu lesen
# ANNAHME: Dein Schlüssel in .streamlit/secrets.toml ist direkt "DATABASE_URL"
# FALLS ER z.B. unter [database] -> url ist, ändere es zu: st.secrets.get("database", {}).get("url")
DATABASE_URL_FROM_SECRETS = st.secrets.get("DATABASE_URL")

# Fallback auf OS-Umgebungsvariable (nützlich für Tests außerhalb von Streamlit)
DATABASE_URL_FROM_OS_ENV = os.getenv("DATABASE_URL")

DATABASE_URL = None # Initialisiere
if DATABASE_URL_FROM_SECRETS:
    DATABASE_URL = DATABASE_URL_FROM_SECRETS
    print("DEBUG [config.py]: DATABASE_URL wurde aus st.secrets verwendet.")
elif DATABASE_URL_FROM_OS_ENV:
    DATABASE_URL = DATABASE_URL_FROM_OS_ENV
    print("DEBUG [config.py]: DATABASE_URL wurde aus OS Umgebungsvariable (os.getenv) verwendet.")

if not DATABASE_URL:
    error_msg = (
        "🛑 KRITISCH: Keine DATABASE_URL in config.py gesetzt.\n"
        "   Für Streamlit: Überprüfe .streamlit/secrets.toml (Schlüssel: 'DATABASE_URL' oder wie von dir definiert).\n"
        "   Für lokale Tests: Setze die OS Umgebungsvariable DATABASE_URL (z.B. via .env und python-dotenv im Testskript)."
    )
    # Um zu sehen, was st.secrets enthält, falls es nicht klappt:
    # print(f"DEBUG [config.py]: Verfügbare Schlüssel in st.secrets: {list(st.secrets.keys())}")
    # if 'supabase' in st.secrets: print(f"DEBUG [config.py]: Inhalt von st.secrets.supabase: {st.secrets.supabase}")
    raise RuntimeError(error_msg)

print(f"INFO [config.py]: DATABASE_URL erfolgreich initialisiert (Auszug): ...{DATABASE_URL[-20:]}") # Zeige etwas mehr von der URL zum Prüfen

# API_KEY analog behandeln, falls nötig
API_KEY = st.secrets.get("API_KEY", os.getenv("API_KEY"))
if not API_KEY:
    print("WARNUNG [config.py]: API_KEY nicht gefunden.")


# === Deine bestehenden allgemeinen Backtest-Einstellungen ===
START_BALANCE = 100_000
NUM_STOCKS = 20
OPTIMIZE_WEIGHTS = True
BACKTEST_WINDOW_DAYS = 200
CSV_PATH = "sample_data/stock_data.csv" # Dieser Pfad ist relativ zum Projekt-Root, wenn von dort ausgeführt

# Rebalancing-Einstellungen
REBALANCE_FREQUENCY = "monthly"
CUSTOM_REBALANCE_MONTHS = 1

# Trading-Kosten Einstellungen
ENABLE_TRADING_COSTS = True
FIXED_COST_PER_TRADE = 1.0
VARIABLE_COST_PCT = 0.000

# === Optimierungsmodus ===
OPTIMIZATION_MODE = "select-then-optimize"

# === Optimierung & Kovarianzschätzung ===
OPTIMIZER_METHOD = "ledoit-wolf"
COV_ESTIMATOR = "ledoit-wolf"

# === Portfolio-Gewichtslimits ===
MIN_WEIGHT = 0.01
MAX_WEIGHT = 0.20

# === Portfolio-Equal wight of Tickers===
FORCE_EQUAL_WEIGHT = False

# === Constraints & Stabilität ===
MAX_TURNOVER = 0.20
MAX_SECTOR_WEIGHT = 0.30

# === Zielbedingungen für Optimierung ===
MIN_CAGR = 0.10
USE_BALANCED_OBJECTIVE = False

# === Benchmark-Einstellungen ===
USE_BENCHMARK = False
BENCHMARK_TICKERS = ["SPY"]

# === Risikomanagement ===
# Hole "enabled" auch aus Secrets, falls es zur Laufzeit änderbar sein soll, sonst Default hier
risk_overlay_enabled_secret = st.secrets.get("RISK_OVERLAY_ENABLED", True) 

RISK_OVERLAY = {
    "enabled": risk_overlay_enabled_secret, 
    "config_path": str(_THIS_DIR / "risk_overlay" / "overlay_config.json"),
    "safe_assets_path": str(_THIS_DIR / "risk_overlay" / "safe_assets.json"),
}
print(f"INFO [config.py]: RISK_OVERLAY enabled: {RISK_OVERLAY['enabled']}")