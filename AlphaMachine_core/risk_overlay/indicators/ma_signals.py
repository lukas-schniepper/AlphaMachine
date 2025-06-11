# In einer neuen Datei ma_signals.py oder in sma.py
import pandas as pd
from ta.trend import SMAIndicator
from ta.trend import SMAIndicatorIndicator

def price_ma_consecutive_signal(eod_data: pd.DataFrame, params: dict) -> pd.Series:
    """
    Signal if close is above/below MA for a consecutive number of days.
    Signal: 1.0 if above MA for 'consecutive' days,
           -1.0 if below MA for 'consecutive' days,
            0.0 otherwise.
    """
    window = params.get('window', 200)
    consecutive_days = params.get('consecutive', 1) # Default zu 1 Tag, wenn nicht spezifiziert

    if 'close' not in eod_data.columns:
        raise ValueError("Input DataFrame 'eod_data' must contain a 'close' column.")

    ma = ta.sma(eod_data['close'], length=window) # oder ema, je nach Präferenz

    above_ma = (eod_data['close'] > ma).astype(int)
    below_ma = (eod_data['close'] < ma).astype(int)

    signal = pd.Series(0.0, index=eod_data.index, dtype=float)

    # Rolling sum to check for consecutive days
    # Check for consecutive days above MA
    # A rolling sum of '1's (for above_ma) will be equal to consecutive_days
    consecutive_above = above_ma.rolling(window=consecutive_days, min_periods=consecutive_days).sum() == consecutive_days
    
    # Check for consecutive days below MA
    consecutive_below = below_ma.rolling(window=consecutive_days, min_periods=consecutive_days).sum() == consecutive_days

    signal[consecutive_above] = 1.0
    signal[consecutive_below] = -1.0 # Wenn "unter MA" ein Risk-Off-Signal sein soll

    # Handle overlaps: If a period ends up being both above and below (sollte nicht bei > < passieren, aber sicher ist sicher)
    # oder wenn eine Umschaltung genau auf den Tag fällt.
    # Die letzte Bedingung, die zutrifft, würde hier gewinnen.
    # Man könnte hier eine Priorität definieren, falls nötig.
    
    return signal.fillna(0.0)