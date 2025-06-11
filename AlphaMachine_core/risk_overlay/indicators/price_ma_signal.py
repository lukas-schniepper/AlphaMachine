# AlphaMachine_core/risk_overlay/indicators/price_ma_signal.py
import pandas as pd
# Importiere die benötigte Indikatorklasse aus der 'ta'-Bibliothek
from ta.trend import SMAIndicator as ta_SMAIndicator # Importiere die SMA-Klasse
# from ta.trend import EMAIndicator as ta_EMAIndicator # Falls du EMA konfigurierbar machen willst
from .base import Indicator

class PriceMAComparisonSignal(Indicator):
    def __init__(self, params: dict):
        super().__init__(params)
        self.ma_period = self.params.get('ma_period', 200)
        self.consecutive_days = self.params.get('consecutive_days', 1)
        self.signal_when_above = self.params.get('signal_when_above', False)
        # Optional: Welchen MA-Typ verwenden? (Standard SMA)
        self.ma_type = self.params.get('ma_type', 'sma').lower()

    def calculate(self, eod_data: pd.DataFrame) -> pd.Series:
        if 'close' not in eod_data.columns:
            raise ValueError("Input DataFrame 'eod_data' must contain a 'close' column.")

        # Berechne den Moving Average mit der 'ta'-Bibliothek
        if self.ma_type == 'sma':
            # Die 'ta' Bibliothek erwartet, dass 'fillna=True' oder 'fillna=False' übergeben wird.
            # Wenn fillna=False, werden NaNs am Anfang erzeugt, was oft besser ist.
            indicator_ma = ta_SMAIndicator(close=eod_data['close'], window=self.ma_period, fillna=False)
            ma_series = indicator_ma.sma_indicator()
        # elif self.ma_type == 'ema': # Beispiel für EMA
        #     indicator_ma = ta_EMAIndicator(close=eod_data['close'], window=self.ma_period, fillna=False)
        #     ma_series = indicator_ma.ema_indicator()
        else:
            raise ValueError(f"Unbekannter MA-Typ: {self.ma_type}. Unterstützt werden 'sma' (oder 'ema' etc.).")

        condition_met_daily = pd.Series(False, index=eod_data.index)
        if self.signal_when_above:
            condition_met_daily = (eod_data['close'] > ma_series)
        else:
            condition_met_daily = (eod_data['close'] < ma_series)
        
        condition_met_daily = condition_met_daily.astype(int) # Konvertiere True/False zu 1/0
        signal = pd.Series(0.0, index=eod_data.index, dtype=float)

        if self.consecutive_days > 0 and self.consecutive_days != 1 : # Rolling Sum macht nur Sinn für > 1 Tag
            # Rolling sum to check for consecutive days
            # Wichtig: rolling().sum() auf einer 0/1 Serie. Wenn Summe == consecutive_days, dann war es so oft 1.
            consecutive_met = condition_met_daily.rolling(window=self.consecutive_days, min_periods=self.consecutive_days).sum() == self.consecutive_days
            signal[consecutive_met] = 1.0
        elif self.consecutive_days == 1: # Direkter Vergleich für 1 Tag
             signal[condition_met_daily == 1] = 1.0
        else: # Wenn consecutive_days <= 0 (oder nicht spezifiziert und Default 1), dann direkter Vergleich
             signal[condition_met_daily == 1] = 1.0
            
        return signal.fillna(0.0) # NaNs füllen, die durch MA-Berechnung oder Rolling entstehen könnten