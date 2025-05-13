# tests/risk_overlay/test_indicator_base.py
from AlphaMachine_core.risk_overlay.indicators.base import IndicatorBase, Mode

def test_default_mode():
    class Dummy(IndicatorBase):
        def calculate(self, data):
            return data["close"] * 0

    d = Dummy()
    assert d.mode == Mode.BOTH
