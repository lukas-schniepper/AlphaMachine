import pandas as pd
from AlphaMachine_core.risk_overlay.overlay import RiskOverlay

class DummyIndicator:
    def __init__(self, score, mode):
        self.score = score
        self.mode = mode
        self.weight = 1.0
    def calculate(self, data):
        return pd.Series([self.score] * len(data))

def test_overlay_aggregation_and_mapping():
    # 2 RiskOn-Indikatoren, einer RiskOff
    overlay = RiskOverlay.__new__(RiskOverlay)
    overlay.indicators = [
        DummyIndicator(0.6, "risk_on"),
        DummyIndicator(0.4, "risk_on"),
        DummyIndicator(0.2, "risk_off"),
    ]
    data = pd.DataFrame({"close": [100,101,102]})
    agg = overlay.aggregate_scores(data)
    eq_weight = overlay.map_to_equity_weight(agg, threshold_on=0.5, threshold_off=0.3)
    # RiskOn dominiert: bei diesen Werten ergibt das Mapping 0.8
    assert agg["risk_on"] == 0.5
    assert abs(eq_weight - 0.8) < 1e-6

def test_overlay_only_riskon():
    overlay = RiskOverlay.__new__(RiskOverlay)
    overlay.indicators = [
        DummyIndicator(1.0, "risk_on"),
        DummyIndicator(0.8, "risk_on"),
    ]
    data = pd.DataFrame({"close": [1,2,3]})
    agg = overlay.aggregate_scores(data)
    eq_weight = overlay.map_to_equity_weight(agg, threshold_on=0.5)
    assert eq_weight == 1.0

def test_overlay_only_riskoff():
    overlay = RiskOverlay.__new__(RiskOverlay)
    overlay.indicators = [
        DummyIndicator(0.7, "risk_off"),
        DummyIndicator(0.9, "risk_off"),
    ]
    data = pd.DataFrame({"close": [1,2,3]})
    agg = overlay.aggregate_scores(data)
    eq_weight = overlay.map_to_equity_weight(agg, threshold_off=0.5)
    assert eq_weight == 0.0

def test_overlay_riskon_equals_riskoff():
    overlay = RiskOverlay.__new__(RiskOverlay)
    overlay.indicators = [
        DummyIndicator(0.5, "risk_on"),
        DummyIndicator(0.5, "risk_off"),
    ]
    data = pd.DataFrame({"close": [1,2,3]})
    agg = overlay.aggregate_scores(data)
    eq_weight = overlay.map_to_equity_weight(agg, threshold_on=0.5, threshold_off=0.5)
    assert abs(eq_weight - 0.5) < 1e-6

def test_overlay_strong_riskoff_overrides():
    overlay = RiskOverlay.__new__(RiskOverlay)
    overlay.indicators = [
        DummyIndicator(0.2, "risk_on"),
        DummyIndicator(1.0, "risk_off"),
    ]
    data = pd.DataFrame({"close": [1,2,3]})
    agg = overlay.aggregate_scores(data)
    eq_weight = overlay.map_to_equity_weight(agg, threshold_on=0.3, threshold_off=0.3)
    assert eq_weight == 0.0

def test_overlay_exact_thresholds():
    overlay = RiskOverlay.__new__(RiskOverlay)
    overlay.indicators = [
        DummyIndicator(0.3, "risk_on"),
    ]
    data = pd.DataFrame({"close": [1,2,3]})
    agg = overlay.aggregate_scores(data)
    eq_weight = overlay.map_to_equity_weight(agg, threshold_on=0.3)
    assert eq_weight == 1.0

def test_overlay_exact_thresholds():
    overlay = RiskOverlay.__new__(RiskOverlay)
    overlay.indicators = [
        DummyIndicator(0.3, "risk_on"),
    ]
    data = pd.DataFrame({"close": [1,2,3]})
    agg = overlay.aggregate_scores(data)
    eq_weight = overlay.map_to_equity_weight(agg, threshold_on=0.3)
    assert eq_weight == 1.0

