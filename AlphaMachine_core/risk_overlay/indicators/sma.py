# AlphaMachine_core/risk_overlay/indicators/sma.py
from .base import IndicatorBase, Mode
import pandas as pd

class SMAIndicator(IndicatorBase):
    """Klassischer Simple Moving Average Indikator."""
    mode = Mode.BOTH

    def __init__(self, period: int = 50):
        self.period = period

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        sma = data["close"].rolling(window=self.period, min_periods=1).mean()
        score = (data["close"] / sma) - 1.0         # >0 ⇒ bullish
        return self.normalize(score)
