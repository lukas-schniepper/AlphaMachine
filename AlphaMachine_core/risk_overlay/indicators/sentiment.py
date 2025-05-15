# AlphaMachine_core/risk_overlay/indicators/sentiment.py
from .base import IndicatorBase, Mode
import pandas as pd

class SentimentZScoreIndicator(IndicatorBase):
    """RiskOn-Indikator: sentiment column, als Z-Score normalisiert."""
    mode = Mode.RISK_ON

    def __init__(self, column: str = "sentiment"):
        self.column = column

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        # Erwartet, dass data[self.column] vorhanden ist!
        score = data[self.column]
        return self.normalize(score)
