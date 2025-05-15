# AlphaMachine_core/risk_overlay/indicator_factory.py
from importlib import import_module
import json

def load_indicators(config_path: str):
    cfg = json.load(open(config_path))
    out = []
    for entry in cfg["indicators"]:
        module = import_module(entry["path"])
        cls = getattr(module, entry["class"])
        ind = cls(**entry.get("params", {}))
        ind.mode = entry.get("mode", ind.mode)
        ind.weight = entry.get("weight", 1.0)
        out.append(ind)
    return out
