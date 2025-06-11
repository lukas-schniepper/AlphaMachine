# AlphaMachine_core/risk_overlay/indicators/ema.py
import pandas as pd
from ta.trend import EMAIndicator

def ema_value(eod_data: pd.DataFrame, params: dict) -> pd.Series:
    """
    Calculates the Exponential Moving Average (EMA) raw value.
    """
    window = params.get('window', 20) # Aus config: "params": {"window": 50}

    if 'close' not in eod_data.columns:
        raise ValueError("Input DataFrame 'eod_data' for EMA must contain a 'close' column.")
        
    ema = ta.ema(eod_data['close'], length=window)
    return ema