# tests/test_hysteresis.py
from AlphaMachine_core.risk_overlay.hysteresis import HysteresisSwitch

def test_hysteresis_switch():
    switch = HysteresisSwitch(low=-0.3, high=0.1)
    assert switch.state  == "risk_on"        # Default-Start
    switch.update(-0.4)                       # unter LOW
    assert switch.state  == "risk_off"
    switch.update(-0.1)                       # dazwischen → bleibt off
    assert switch.state  == "risk_off"
    switch.update(0.2)                        # über HIGH
    assert switch.state  == "risk_on"
