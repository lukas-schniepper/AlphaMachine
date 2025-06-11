# AlphaMachine_core/risk_overlay/indicators/sentiment.py
import pandas as pd
from .base import Indicator
# Wir brauchen unsere z_score_scaler Funktion, die wir zuvor definiert haben.
# Diese muss importierbar sein. Angenommen, sie ist in utils.normalization
from ..utils.normalization import z_score_scaler # Relativer Import zum übergeordneten utils

class SentimentZScoreIndicator(Indicator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.data_column = self.params.get('column', 'sentiment_value') # 'column' aus deiner Config
        self.lookback = self.params.get('lookback', 104) # Standard-Lookback für Z-Score

    def calculate(self, eod_data: pd.DataFrame) -> pd.Series:
        # eod_data hier sind die Sentiment-Daten für das konfigurierte "asset"
        if self.data_column not in eod_data.columns:
            # raise ValueError(f"Sentiment data column '{self.data_column}' not found.")
            print(f"WARNUNG: Sentiment-Spalte '{self.data_column}' nicht gefunden. Gebe Nullen zurück.")
            return pd.Series(0.0, index=eod_data.index, dtype=float)

        sentiment_series = eod_data[self.data_column]
        
        # Z-Score direkt hier berechnen
        z_score_values = z_score_scaler(sentiment_series, lookback_period=self.lookback)
        
        return z_score_values # Gibt den Z-Score zurück