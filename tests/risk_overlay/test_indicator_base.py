from AlphaMachine_core.risk_overlay.indicators.base import IndicatorBase

def test_default_mode():
    class Dummy(IndicatorBase):
        def calculate(self, data):
            return data["close"]*0
    assert Dummy().mode == Dummy.mode.BOTH

