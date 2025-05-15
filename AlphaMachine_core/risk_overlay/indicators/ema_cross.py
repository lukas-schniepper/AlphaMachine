# AlphaMachine_core/risk_overlay/indicators/ema_cross.py
from .base import IndicatorBase, Mode
import pandas as pd

class EMACrossIndicator(IndicatorBase):
    """RiskOn, wenn EMA fast / stark > EMA langsam; sonst RiskOff."""
    mode = Mode.BOTH

    def __init__(self, fast: int = 50, slow: int = 200):
        self.fast = fast
        self.slow = slow

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        ema_fast = data["close"].ewm(span=self.fast, adjust=False).mean()
        ema_slow = data["close"].ewm(span=self.slow, adjust=False).mean()
        # Score: >0 bullish (RiskOn), <0 bearish (RiskOff)
        score = ema_fast - ema_slow
        return self.normalize(score)
