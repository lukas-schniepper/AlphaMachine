# config.py – zentrale Parameterdatei für den Backtest
import os

DATABASE_URL = os.getenv("DATABASE_URL")

# === Allgemeine Backtest-Einstellungen ===
START_BALANCE = 100_000
NUM_STOCKS = 20
OPTIMIZE_WEIGHTS = True
BACKTEST_WINDOW_DAYS = 200
CSV_PATH = "sample_data/stock_data.csv"

# Rebalancing-Einstellungen
REBALANCE_FREQUENCY = "monthly"  # "weekly", "monthly", oder ein spezifischer Wert in Monaten, z.B. 3 für quartalsweise
CUSTOM_REBALANCE_MONTHS = (
    1  # Wird genutzt, wenn REBALANCE_FREQUENCY nicht "weekly" oder "monthly" ist
)

# Trading-Kosten Einstellungen
ENABLE_TRADING_COSTS = True  # Trading-Kosten ein-/ausschalten
FIXED_COST_PER_TRADE = 1.0  # Fixer Betrag pro Trade in der Währung deines Portfolios
VARIABLE_COST_PCT = (
    0.000  # Variable Kosten als Prozentsatz des Handelsvolumens (0.001 = 0.1%)
)

# === Optimierungsmodus ===
# "select-then-optimize" = erst 20 Ticker auswählen, dann gewichten
# "optimize-subset" = Optimierer wählt aus z. B. 150 Titeln selbst die besten 20
OPTIMIZATION_MODE = "select-then-optimize"  # oder "optimize-subset"

# === Optimierung & Kovarianzschätzung ===
OPTIMIZER_METHOD = "ledoit-wolf"  # z. B. 'ledoit-wolf', 'minvar', 'hrp'
COV_ESTIMATOR = "ledoit-wolf"  # später: 'constant-corr', 'factor-model'

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

# === Kovarianzschätzer ===
COV_ESTIMATOR = (
    "ledoit-wolf"  # Optionen: "ledoit-wolf", "constant-corr", "factor-model"
)
