import pandas as pd
from AlphaMachine_core.risk_overlay.indicators.sentiment import SentimentZScoreIndicator

def test_sentiment_indicator_zscore():
    # Simulierter Sentiment-Verlauf (zwei Ausreißer)
    data = pd.DataFrame({"sentiment": [0.1, 0.2, 0.1, 0.2, 2.0]})
    ind = SentimentZScoreIndicator(column="sentiment")
    score = ind.calculate(data)
    # Der höchste Wert wird als größter Z-Score erkannt
    assert score.idxmax() == 4
    # Die Z-Scores um den Mittelwert herum liegen nahe 0
    assert abs(score.iloc[0]) < 1.5
