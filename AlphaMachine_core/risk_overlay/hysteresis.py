# AlphaMachine_core/risk_overlay/hysteresis.py
"""
Zweistufige State-Maschine mit Hysterese und Mindest-Verweildauer.

Begriffe
--------
* **state**      : "risk_on"  ↔  "risk_off"
* **low  / high**: Schwellen, bei deren Über-/Unterschreitung
                   (nach `min_days`) gewechselt wird.
"""

from __future__ import annotations


class HysteresisSwitch:
    """
    Hysterese-Schalter zur Glättung binärer Signale.

    Parameters
    ----------
    low : float
        Unterschwelle für den Wechsel von *risk_on* → *risk_off*.
    high : float
        Oberschwelle für den Wechsel von *risk_off* → *risk_on*.
    min_days : int, default 1
        So viele **aufeinander­folgende** Tage muss die Schwelle verletzt
        sein, bevor der Zustand tatsächlich wechselt.
    initial_state : {"risk_on", "risk_off"}, default "risk_on"
        Startzustand.
    """

    def __init__(
        self,
        *,
        low: float,
        high: float,
        min_days: int = 1,
        initial_state: str = "risk_on",
    ) -> None:
        if low >= high:
            raise ValueError("`low` muss kleiner als `high` sein")
        if initial_state not in ("risk_on", "risk_off"):
            raise ValueError("initial_state muss 'risk_on' oder 'risk_off' sein")
        if min_days < 1:
            raise ValueError("min_days muss ≥ 1 sein")

        self.low = float(low)
        self.high = float(high)
        self.min_days = int(min_days)

        self.state: str = initial_state          # öffentlich – von Tests genutzt
        self._counter: int = 0                   # zählt Tage über / unter Schwelle

    # ------------------------------------------------------------------ #
    # API
    # ------------------------------------------------------------------ #
    def update(self, score: float) -> str:
        """
        Neuen Score einfüttern; liefert den ggf. geänderten Zustand zurück.
        """
        score = float(score)

        # Prüfen, ob Schwelle „in die andere Richtung“ verletzt wird
        if self.state == "risk_on":
            threshold_crossed = score <= self.low
        else:  # risk_off
            threshold_crossed = score >= self.high

        # Counter führen
        self._counter = self._counter + 1 if threshold_crossed else 0

        # Wechsel nach min_days Bestätigung
        if self._counter >= self.min_days:
            self.state = "risk_off" if self.state == "risk_on" else "risk_on"
            self._counter = 0

        return self.state

    # Komfort-Property (True = Aktienquote 100 %)
    # --------------------------------------------------------------
    @property
    def is_risk_on(self) -> bool:  # optional, nicht test-relevant
        return self.state == "risk_on"


__all__ = ["HysteresisSwitch"]
