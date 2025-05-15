from abc import ABC, abstractmethod
from enum import Enum
import pandas as pd
import numpy as np

class Mode(str, Enum):
    RISK_OFF = "risk_off"
    RISK_ON  = "risk_on"
    BOTH     = "both"

class IndicatorBase(ABC):
    """
    Grundklasse für alle Risk-Overlay-Indikatoren.
    """

    mode: Mode = Mode.BOTH  # kann via JSON-Konfig überschrieben werden

    @abstractmethod
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """
        Liefert den (normalisierten) Score pro Datum.
        """
        raise NotImplementedError

    def confidence(self, score: pd.Series) -> pd.Series:
        """
        Optional – liefert ein Confidence-Band (0–1). Default = 1.
        """
        return pd.Series(1.0, index=score.index)
    
    def normalize(self, series: pd.Series) -> pd.Series:
        """Normiert Scores als Z-Score (Standardisierung)"""
        mu = series.mean()
        sigma = series.std()
        if sigma == 0 or np.isnan(sigma):
            return series * 0  # oder pd.Series(0, index=series.index)
        return (series - mu) / sigma
