import pandas as pd
from AlphaMachine_core.risk_overlay.indicators.ema_cross import EMACrossIndicator

def test_ema_cross_score_positive_when_fast_above_slow():
    data = pd.DataFrame({"close": [100]*200 + [120]*10})
    ind = EMACrossIndicator(fast=5, slow=10)
    score = ind.calculate(data)
    # Letzter Wert deutlich positiv (Kurs über beiden EMAs)
    assert score.iloc[-1] > 0.5
    # Anfangswert sollte "fast konstant" und nahe 0 sein
    assert abs(score.iloc[0]) < 0.25
