from abc import ABC, abstractmethod
from enum import Enum
import pandas as pd

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
