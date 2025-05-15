from typing import Dict, List
import pandas as pd

class RiskOverlay:
    """
    Aggregiert Risk-On/Off-Signale zu einer Ziel-Aktienquote
    und passt die Orderliste an.
    """

    def __init__(self, indicators: List):
        self.indicators = indicators

    def score(self, data: pd.DataFrame) -> pd.DataFrame:
        # getrennte Aggregation
        scores = {ind: ind.calculate(data) for ind in self.indicators}
        # TODO: Score-Normalisierung & Gewichtung
        return pd.DataFrame(scores)

    def apply(self, date, base_orders: Dict) -> Dict:
        """
        Manipuliert die geplanten Orders (Aktien ↔ Safe-Assets).
        """
        # TODO: Time-in-State, Mapping, Hysterese
        return base_orders
