# scripts/test_load_indicators.py
import json
from AlphaMachine_core.risk_overlay.indicator_factory import load_indicator

with open("tests/risk_overlay/overlay_config.json") as f:
    cfg = json.load(f)

indicators = [
    load_indicator(entry["path"], entry["class"], **entry.get("params", {}))
    for entry in cfg["indicators"]
]

print(indicators)
for ind in indicators:
    print(ind, ind.mode)
