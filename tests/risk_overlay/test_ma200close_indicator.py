import pandas as pd
from AlphaMachine_core.risk_overlay.indicators.ma200 import MA200CloseIndicator

def test_ma200close_riskoff():
    df = pd.DataFrame({"close": [100]*200 + [90, 89, 88]})  # letzten 3 Tage unter MA200
    ind = MA200CloseIndicator(ma_period=200, days_below=3)
    signal = ind.calculate(df)
    assert signal.iloc[-1] == 1  # RiskOff ausgelöst
