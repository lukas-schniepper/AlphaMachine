# tests/test_mapping.py
import pytest
from AlphaMachine_core.risk_overlay.mapping import map_score_to_weight

# ---------- LINEAR --------------------------------------------------
@pytest.mark.parametrize(
    "score, expected",
    [(-1, 0.0), (0, 0.5), (1, 1.0), (-2, 0.0), (2, 1.0)],
)
def test_linear_mapping(score, expected):
    cfg = {"type": "linear", "params": {}}
    assert map_score_to_weight(score, cfg) == pytest.approx(expected, rel=1e-3)


# ---------- SIGMOID -------------------------------------------------
def test_sigmoid_monotonic():
    cfg = {"type": "sigmoid", "params": {"k": 8}}
    w1 = map_score_to_weight(-0.5, cfg)
    w2 = map_score_to_weight(0.0, cfg)
    w3 = map_score_to_weight(0.5, cfg)
    assert 0 <= w1 < w2 < w3 <= 1


# ---------- THREE-BAND ---------------------------------------------
@pytest.mark.parametrize(
    "score, expected",
    [(-0.6, 0.0), (-0.1, 0.5), (0.4, 1.0)],
)
def test_three_band_mapping(score, expected):
    cfg = {
        "type": "three_band",
        "params": {"low": -0.5, "high": 0.3}
    }
    cfg = {"type": "three_band", "params": {"low": -0.5, "high": 0.3}}
    assert map_score_to_weight(score, cfg) == expected

