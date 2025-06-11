# AlphaMachine_core/risk_overlay/indicators/base.py
from abc import ABC, abstractmethod
from enum import Enum
import pandas as pd
import numpy as np # Behalte numpy für np.isnan und ggf. andere Operationen
from typing import Dict, Any # Für Typ-Annotationen

class Mode(str, Enum):
    RISK_OFF = "risk_off"
    RISK_ON  = "risk_on"
    BOTH     = "both"

class Indicator(ABC):
    """
    Grundklasse für alle Risk-Overlay-Indikatoren.
    """

    # mode: Mode = Mode.BOTH # Dieses Klassenattribut kann als Default dienen,
                           # aber die tatsächliche Mode-Zuweisung erfolgt besser
                           # über die Konfiguration und wird in der RiskOverlay-Klasse gehandhabt.
                           # Wir können es hier lassen, aber es wird von der Config überschrieben.

    def __init__(self, params: Dict[str, Any]): # <<< HIER IST DIE WICHTIGE ERGÄNZUNG
        """
        Initialisiert den Indikator mit spezifischen Parametern.
        'params' ist ein Dictionary, das aus der Konfigurationsdatei kommt.
        """
        self.params = params
        # Du könntest hier auch allgemeine Parameter extrahieren, die für alle Indikatoren gelten,
        # falls du sie in der Basisklasse verarbeiten willst, z.B.:
        # self.window = self.params.get('window', 20) # Beispiel, wenn alle Indikatoren ein 'window' hätten

    @abstractmethod
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        Liefert den Roh-Signalwert (oder einen vor-normalisierten Wert) pro Datum.
        Die Normalisierung und Anwendung von 'direction' erfolgt später in der RiskOverlay-Klasse.
        """
        raise NotImplementedError

    def confidence(self, score: pd.Series) -> pd.Series:
        """
        Optional – liefert ein Confidence-Band (0–1). Default = 1.
        Diese Methode wird derzeit in unserem RiskOverlay-Flow noch nicht aktiv genutzt,
        aber es ist gut, sie für spätere Erweiterungen zu haben.
        """
        return pd.Series(1.0, index=score.index)
    
    def normalize(self, series: pd.Series) -> pd.Series:
        """
        Normiert Scores als Z-Score (Standardisierung).
        Diese Methode wird derzeit in unserem RiskOverlay-Flow noch nicht direkt
        von den Indikator-Instanzen aufgerufen, da die Normalisierung zentral
        in RiskOverlay.get_processed_indicator_signals gehandhabt wird,
        basierend auf der 'transform'-Einstellung in der Config.
        Sie könnte aber als Hilfsfunktion oder für Indikatoren dienen, die
        eine spezifische interne Normalisierung benötigen.
        """
        mu = series.mean()
        sigma = series.std()
        if sigma == 0 or np.isnan(sigma) or sigma == 0.0: # Sicherere Prüfung
            return pd.Series(0.0, index=series.index, dtype=float) # Gib eine Serie von Nullen zurück
        return (series - mu) / sigma