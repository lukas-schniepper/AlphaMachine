# AlphaMachine_core/risk_overlay/indicators/__init__.py

# Importiere alle Indikator-Klassen, damit sie über diesen Namespace verfügbar sind,
# falls die RiskOverlay-Klasse sie dynamisch über getattr oder importlib laden muss.

from .base import Indicator # Basisklasse könnte nützlich sein
from .sma import SMAIndicator
from .ema_cross import EMACrossIndicator
from .sentiment import SentimentZScoreIndicator
from .drawdown_from_high import DrawDownFromHighIndicator
from .rsi import RSIIndicator # Neu
from .vix import VIXThresholdIndicator # Neu
from .price_ma_signal import PriceMAComparisonSignal

# EMAIndicator, falls du einen reinen EMA-Wert brauchst (aktuell nicht in deiner Config)
# from .ema import EMAIndicator