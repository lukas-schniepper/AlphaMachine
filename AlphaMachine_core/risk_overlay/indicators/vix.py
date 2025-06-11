# AlphaMachine_core/risk_overlay/indicators/vix.py
import pandas as pd
from .base import Indicator

class VIXThresholdIndicator(Indicator): # Angenommener Klassenname
    def __init__(self, params: dict):
        super().__init__(params)
        self.level = self.params.get('level', 30)
        self.condition_above = self.params.get('condition_above', True)

    def calculate(self, eod_data: pd.DataFrame) -> pd.Series:
        if 'close' not in eod_data.columns:
            raise ValueError("Input DataFrame 'eod_data' (for VIX) must contain a 'close' column.")

        signal = pd.Series(0.0, index=eod_data.index, dtype=float)

        if self.condition_above:
            signal[eod_data['close'] > self.level] = 1.0
        else:
            signal[eod_data['close'] < self.level] = 1.0
            
        return signal.fillna(0.0)