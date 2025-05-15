import pandas as pd
from AlphaMachine_core.risk_overlay.indicators.ema import EMAIndicator

def test_ema_indicator_sign():
    df = pd.DataFrame({"close": [100, 101, 102, 110]})
    ind = EMAIndicator(period=2)
    scores = ind.calculate(df)
    assert scores.iloc[-1] > 0          # letzter Wert positiv
