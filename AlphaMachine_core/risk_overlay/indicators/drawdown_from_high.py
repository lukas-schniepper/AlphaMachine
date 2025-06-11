# AlphaMachine_core/risk_overlay/indicators/drawdown_from_high.py
import pandas as pd
from .base import Indicator

class DrawDownFromHighIndicator(Indicator):
    def __init__(self, params: dict):
        super().__init__(params)
        # Deine Config hat threshold_1, threshold_2. Sollen diese hier direkt Signale erzeugen?
        # Oder soll der Indikator den Drawdown-Wert liefern und Schwellen kommen später?
        # Ich nehme an, wir liefern den Drawdown-Wert. Die Thresholds müssten dann
        # in der Normalisierungs/Transformationslogik oder einer speziellen Signalfunktion genutzt werden.
        # Für jetzt: Wir liefern den Drawdown-Wert. 'window' fehlt in deiner Config, fügen wir hinzu.
        self.window = self.params.get('window', 252)
        # Die Thresholds werden hier noch nicht verwendet, es sei denn, die Klasse soll direkt Signale geben.

    def calculate(self, eod_data: pd.DataFrame) -> pd.Series:
        if 'close' not in eod_data.columns:
            raise ValueError("Input DataFrame 'eod_data' for Drawdown must contain a 'close' column.")

        rolling_high = eod_data['close'].rolling(window=self.window, min_periods=1).max()
        drawdown = (eod_data['close'] - rolling_high) / rolling_high
        
        return drawdown.fillna(0.0) # Drawdown ist <= 0