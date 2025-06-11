# AlphaMachine_core/risk_overlay/indicators/sma.py
import pandas as pd
from ta.trend import SMAIndicator
from .base import Indicator # Wichtig: von base.Indicator erben



class SMAIndicator(Indicator): # Klassenname muss mit Config übereinstimmen
    def __init__(self, params: dict):
        super().__init__(params) # Ruft __init__ der Basisklasse auf
        self.window = self.params.get('period', 20) # 'period' aus deiner Config

    def calculate(self, eod_data: pd.DataFrame) -> pd.Series:
        if 'close' not in eod_data.columns:
            raise ValueError("Input DataFrame 'eod_data' for SMA must contain a 'close' column.")
        
        sma = ta.sma(eod_data['close'], length=self.window)
        return sma # Rohwert zurückgeben, Normalisierung später