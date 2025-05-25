import json
import numpy as np
from AlphaMachine_core.risk_overlay.indicator_factory import load_indicator

class RiskOverlay:
    def __init__(self, config_path: str):
        # Indikatoren aus Konfig laden
        with open(config_path) as f:
            cfg = json.load(f)
        self.indicators = [
            load_indicator(entry["path"], entry["class"], **entry.get("params", {}))
            for entry in cfg["indicators"]
        ]
        # Optionale Attribute aus der Config setzen
        for i, entry in enumerate(cfg["indicators"]):
            if "mode" in entry:
                self.indicators[i].mode = entry["mode"]
            if "weight" in entry:
                self.indicators[i].weight = entry["weight"]

    def get_scores(self, data):
        """
        Berechnet für alle Indikatoren die Scores auf Basis von data (z.B. DataFrame).
        Gibt eine Liste von (Score, Mode, Weight) pro Indikator zurück.
        """
        scores = []
        for ind in self.indicators:
            score = ind.calculate(data)
            mode = getattr(ind, "mode", "both")
            weight = getattr(ind, "weight", 1.0)
            scores.append((score, mode, weight))
        return scores

    def aggregate_scores(self, data):
        """
        Aggregiert alle Indikatoren getrennt nach mode (risk_on, risk_off, both),
        gibt dict mit aggregierten Scores zurück.
        """
        score_on = []
        score_off = []
        score_both = []

        for ind in self.indicators:
            score = ind.calculate(data)
            mode = getattr(ind, "mode", "both")
            weight = getattr(ind, "weight", 1.0)
            # Die letzten Werte nehmen, falls Series kommt:
            score_last = score.iloc[-1] if hasattr(score, "iloc") else score
            if mode == "risk_on":
                score_on.append(weight * score_last)
            elif mode == "risk_off":
                score_off.append(weight * score_last)
            else:  # both
                score_both.append(weight * score_last)
        # Aggregation: Mittelwert (kann auch Summe, Median etc. sein)
        agg = {
            "risk_on": np.mean(score_on) if score_on else 0.0,
            "risk_off": np.mean(score_off) if score_off else 0.0,
            "both": np.mean(score_both) if score_both else 0.0
        }
        return agg
    
    def map_to_equity_weight(self, agg_scores, threshold_on=0.3, threshold_off=0.3):
        """
        Aggregierte Scores auf Ziel-Aktienquote abbilden (zwischen 0 und 1).
        threshold_on: ab welchem Wert 100% Aktien (RiskOn)?
        threshold_off: ab welchem Wert 0% Aktien (RiskOff)?
        Dazwischen linear interpoliert.
        """
        overlay_score = agg_scores["risk_on"] - agg_scores["risk_off"] + agg_scores["both"]
        # Mapping:
        if overlay_score >= threshold_on:
            return 1.0   # 100% Aktien
        elif overlay_score <= -threshold_off:
            return 0.0   # 0% Aktien
        else:
            # Linear zwischen Schwellenwerten (0–1)
            # Beispiel: overlay_score = 0: 50% Aktien
            return 0.5 + overlay_score / (2 * threshold_on)
