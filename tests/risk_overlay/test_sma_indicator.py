import pandas as pd
from AlphaMachine_core.risk_overlay.indicators.sma import SMAIndicator

def test_sma_positive_when_price_above_sma():
    df = pd.DataFrame({"close": [10, 12, 14, 20]})
    ind = SMAIndicator(period=2)
    s = ind.calculate(df)
    assert s.iloc[-1] > 0
