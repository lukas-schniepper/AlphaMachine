# AlphaMachine_core/risk_overlay/mapping.py
"""
Mapping-Funktionen für den Risk-On / Risk-Off-Overlay.

- Jeder Score wird als Float im Bereich [-1, +1] erwartet.
- Rückgabe ist eine Aktienquote 0 … 1
  (0 = voll risk_off, 1 = voll risk_on).
"""

from __future__ import annotations

import math
from typing import Callable, Literal

import numpy as np

# --------------------------------------------------------------------------- #
# Typ-Alias
# --------------------------------------------------------------------------- #
MappingType = Literal["linear", "sigmoid", "three_band"]

# --------------------------------------------------------------------------- #
# Low-Level-Mapper
# --------------------------------------------------------------------------- #
def linear(score: float, lo: float = -1.0, hi: float = 1.0) -> float:
    """Einfache lineare Skalierung."""
    return float(np.clip((score - lo) / (hi - lo), 0.0, 1.0))


def sigmoid(score: float, k: float = 10.0) -> float:
    """
    Sigmoid-Mapping.

    Parameter
    ---------
    k : float
        Steilheit der S-Kurve (je größer, desto steiler).
    """
    return 1.0 / (1.0 + math.exp(-k * score))


def three_band(score: float, low: float = -0.5, high: float = 0.5) -> float:
    """
    Drei-Zonen-Mapping:

    * score ≤ low   → 0 % Aktien
    * score ≥ high  → 100 % Aktien
    * dazwischen    → linearer Übergang
    """
    if low >= high:
        raise ValueError("`low` muss kleiner als `high` sein")

    if score <= low:
        return 0.0
    if score >= high:
        return 1.0

    # Linearer Anteil zwischen low und high
    return (score - low) / (high - low)


# --------------------------------------------------------------------------- #
# High-Level Factory – wird in den Tests aufgerufen
# --------------------------------------------------------------------------- #
_MAPPER_DISPATCH: dict[MappingType, Callable[..., float]] = {
    "linear": linear,
    "sigmoid": sigmoid,
    "three_band": three_band,
}


def map_score_to_weight(score: float, cfg: dict | None = None) -> float:
    """
    Factory-Wrapper, ruft anhand einer Config den passenden Mapper auf.

    Beispiele
    ---------
    >>> map_score_to_weight(0.2)                                   # linear default
    >>> map_score_to_weight(0.2, {"type": "sigmoid", "params": {}})
    >>> map_score_to_weight(
    ...     0.1,
    ...     {"type": "three_band", "params": {"low": -0.2, "high": 0.4}},
    ... )
    """
    if cfg is None:
        cfg = {"type": "linear", "params": {}}

    map_type: MappingType = cfg.get("type", "linear")  # type: ignore[arg-type]
    params: dict = cfg.get("params", {})

    if map_type not in _MAPPER_DISPATCH:
        raise ValueError(f"Unbekannter Mapping-Typ: {map_type}")

    # Score vorsorglich clippen → stabilere Gradienten bei Sigmoid etc.
    score = max(-1.0, min(1.0, float(score)))

    return float(_MAPPER_DISPATCH[map_type](score, **params))


__all__ = [
    "linear",
    "sigmoid",
    "three_band",
    "map_score_to_weight",
]
