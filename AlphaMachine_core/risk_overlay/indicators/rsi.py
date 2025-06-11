# AlphaMachine_core/risk_overlay/indicators/rsi.py
import pandas as pd
from ta.momentum import RSIIndicator
from .base import Indicator

class RSIIndicator(Indicator): # Angenommener Klassenname
    def __init__(self, params: dict):
        super().__init__(params)
        self.period = self.params.get('period', 14)

    def calculate(self, eod_data: pd.DataFrame) -> pd.Series:
        if 'close' not in eod_data.columns:
            raise ValueError("Input DataFrame 'eod_data' for RSI must contain a 'close' column.")
        
        rsi = ta.rsi(eod_data['close'], length=self.period)
        return rsi # Rohwert 0-100