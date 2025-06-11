# AlphaMachine_core/risk_overlay/indicators/ema_cross.py
import pandas as pd
from .base import Indicator
from ta.trend import EMAIndicator


class EMACrossIndicator(Indicator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.fast_window = self.params.get('fast', 50)
        self.slow_window = self.params.get('slow', 200)

    def calculate(self, eod_data: pd.DataFrame) -> pd.Series:
        if 'close' not in eod_data.columns:
            raise ValueError("Input DataFrame 'eod_data' for EMA Crossover must contain a 'close' column.")

        fast_ema = ta.ema(eod_data['close'], length=self.fast_window)
        slow_ema = ta.ema(eod_data['close'], length=self.slow_window)
        
        signal = pd.Series(0.0, index=eod_data.index, dtype=float)

        if fast_ema is not None and slow_ema is not None:
            valid_fast = ~fast_ema.isna()
            valid_slow = ~slow_ema.isna()
            common_valid = valid_fast & valid_slow
            
            signal[common_valid & (fast_ema > slow_ema)] = 1.0
            signal[common_valid & (fast_ema < slow_ema)] = -1.0
        
        return signal # fillna(0) am Ende in der Overlay-Klasse