import pandas as pd
import numpy as np

def min_max_scaler(series: pd.Series, feature_range=(-1, 1), lookback_period: int = 0) -> pd.Series:
    """
    Scales a pandas Series to a given feature range using Min-Max scaling.
    A rolling lookback_period can be used for min/max calculation.
    If lookback_period is 0 or less, uses all available data.
    """
    min_val, max_val = feature_range
    if lookback_period > 0:
        # Ensure min_periods is at least 1 and not more than lookback_period
        min_p = max(1, min(lookback_period, lookback_period // 2 if lookback_period > 1 else 1))
        rolling_min = series.rolling(window=lookback_period, min_periods=min_p).min()
        rolling_max = series.rolling(window=lookback_period, min_periods=min_p).max()
    else:
        rolling_min = series.min() # Global min
        rolling_max = series.max() # Global max

    range_val = rolling_max - rolling_min
    
    scaled_series = series.copy()
    
    # Handle cases where range_val is zero (flat line)
    # Set to the middle of the feature_range or min_val as a neutral signal
    zero_range_mask = (range_val == 0)
    
    # Apply scaling where range_val is not zero
    non_zero_range_mask = ~zero_range_mask
    
    # Ensure rolling_min, rolling_max, and range_val are aligned with series for broadcasting
    # If rolling_min/max are single values (global), they broadcast fine.
    # If they are Series (rolling), ensure they align.
    
    if isinstance(rolling_min, pd.Series): # Rolling calculation
        scaled_series[non_zero_range_mask] = (series[non_zero_range_mask] - rolling_min[non_zero_range_mask]) / range_val[non_zero_range_mask] * (max_val - min_val) + min_val
        scaled_series[zero_range_mask] = min_val + (max_val - min_val) / 2 
    else: # Global calculation
        if range_val != 0:
            scaled_series = (series - rolling_min) / range_val * (max_val - min_val) + min_val
        else:
            scaled_series[:] = min_val + (max_val - min_val) / 2

    scaled_series = scaled_series.clip(lower=min_val, upper=max_val)
    return scaled_series

def z_score_scaler(series: pd.Series, lookback_period: int = 0) -> pd.Series:
    """
    Scales a pandas Series using Z-score normalization.
    A rolling lookback_period can be used for mean/std calculation.
    If lookback_period is 0 or less, uses all available data.
    """
    if lookback_period > 0:
        min_p = max(1, min(lookback_period, lookback_period // 2 if lookback_period > 1 else 1))
        mean = series.rolling(window=lookback_period, min_periods=min_p).mean()
        std = series.rolling(window=lookback_period, min_periods=min_p).std()
    else:
        mean = series.mean()
        std = series.std()

    scaled_series = series.copy()
    
    zero_std_mask = (std == 0)
    non_zero_std_mask = ~zero_std_mask

    if isinstance(std, pd.Series): # Rolling calculation
        scaled_series[non_zero_std_mask] = (series[non_zero_std_mask] - mean[non_zero_std_mask]) / std[non_zero_std_mask]
        scaled_series[zero_std_mask] = 0.0 
    else: # Global calculation
        if std != 0:
            scaled_series = (series - mean) / std
        else:
            scaled_series[:] = 0.0
    return scaled_series

def binary_scaler(series: pd.Series, threshold: float = 0.0, condition_above: bool = True) -> pd.Series:
    """
    Converts a series to binary (0 or 1) based on a threshold.
    If condition_above is True, values > threshold become 1, else 0.
    If condition_above is False, values < threshold become 1, else 0.
    """
    if condition_above:
        return (series > threshold).astype(float) # Use float for consistency
    else:
        return (series < threshold).astype(float) # Use float for consistency