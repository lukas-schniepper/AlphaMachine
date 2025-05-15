# AlphaMachine_core/risk_overlay/indicators/ma200.py
from .base import IndicatorBase, Mode
import pandas as pd

class MA200CloseIndicator(IndicatorBase):
    """RiskOff: Preis schließt 3 Tage unter MA200."""
    mode = Mode.RISK_OFF

    def __init__(self, ma_period: int = 200, days_below: int = 3):
        self.ma_period = ma_period
        self.days_below = days_below

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        ma = data["close"].rolling(window=self.ma_period, min_periods=1).mean()
        below = data["close"] < ma
        signal = below.rolling(window=self.days_below).sum() == self.days_below
        # Score: 1 = RiskOff aktiv, 0 = nicht aktiv
        return signal.astype(float)
