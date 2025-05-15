from .base import IndicatorBase, Mode
import pandas as pd

class EMAIndicator(IndicatorBase):
    """Exponentieller Gleitender Durchschnitt – liefert positi­ven Score, wenn Kurs > EMA."""
    mode = Mode.BOTH      # via JSON umstellbar

    def __init__(self, period: int = 50):
        self.period = period

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        ema = data["close"].ewm(span=self.period, adjust=False).mean()
        score = (data["close"] / ema) - 1.0          # >0 ⇒ bullish
        return self.normalize(score)
