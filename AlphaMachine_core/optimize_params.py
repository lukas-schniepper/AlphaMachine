"""
Optuna‑Wrapper für AlphaMachine‑Backtests
----------------------------------------
* search_space   – dict wie {'num_stocks':('int',5,50,1), 'cov_estimator':('categorical',['ledoit‑wolf',…])}
* fixed_kwargs   – alle Engine‑Argumente, die **nicht** optimiert werden
"""

from __future__ import annotations
import optuna
import pandas as pd
from AlphaMachine_core.engine import SharpeBacktestEngine


# ──────────────────────────────────────────────────────────────────────────────
# KPI‑Aggregation  ➟  single scalar score
# ──────────────────────────────────────────────────────────────────────────────
def kpi_objective(weights: dict[str, float], metrics: dict[str, float]) -> float:
    score = 0.0
    for kpi, w in weights.items():
        val = metrics.get(kpi)
        if val is not None and not pd.isna(val):
            score += w * val
    return score


# ------------------------------------------------------------
# 2) Objective‑Funktion  (einzige gültige Version!)
# ------------------------------------------------------------
def objective(
    trial: optuna.Trial,
    price_df: pd.DataFrame,
    fixed_kwargs: dict,
    search_space: dict,
    kpi_weights: dict[str, float],
) -> float:
    """
    Baut kwargs dynamisch aus `search_space` + `fixed_kwargs`,
    legt die wichtigsten KPIs als trial.user_attr ab und liefert
    den zusammengesetzten Score zurück.
    """

    # ---------- A) Parameter aus dem Suchraum --------------------
    kwargs = fixed_kwargs.copy()

    for name, spec in search_space.items():
        kind = spec[0]

        if kind == "int":
            _, lo, hi, step = spec
            kwargs[name] = trial.suggest_int(name, lo, hi, step=step)
        elif kind == "float":
            _, lo, hi, step = spec
            kwargs[name] = trial.suggest_float(name, lo, hi, step=step)
        elif kind == "categorical":
            _, options = spec
            kwargs[name] = trial.suggest_categorical(name, options)
        else:
            raise ValueError(f"Unbekannter Parametertyp '{kind}' für {name}")

    # ---------- B) Backtest ausführen ----------------------------
    eng = SharpeBacktestEngine(price_df, **kwargs)
    eng.run_with_next_month_allocation()

    if eng.performance_metrics.empty:
        raise optuna.TrialPruned("empty performance metrics")

    # ---------- C) KPIs einsammeln -------------------------------
    pf = (
        eng.performance_metrics
           .set_index("Metric")["Value"]
           .str.replace(r"[%\$ ,]", "", regex=True)
           .astype(float)
           .to_dict()
    )

    # → in Optuna speichern, damit sie in trials_dataframe() auftauchen
    trial.set_user_attr("Sharpe",       pf.get("Sharpe Ratio"))
    trial.set_user_attr("CAGR",         pf.get("CAGR (%)"))
    trial.set_user_attr("Ulcer Index",  pf.get("Ulcer Index"))

    # ---------- D) Zielwert zurückgeben --------------------------
    return kpi_objective(kpi_weights, pf)


# ──────────────────────────────────────────────────────────────────────────────
# Public Helper  ➟  in Streamlit aufrufen
# ──────────────────────────────────────────────────────────────────────────────
def run_optimizer(
    price_df:     pd.DataFrame,
    fixed_kwargs: dict,
    search_space: dict,
    kpi_weights:  dict[str, float],
    n_trials: int = 50,
    timeout:  int | None = None,
):
    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=42),
    )

    # hübscher Progress‑Balken in Streamlit
    import streamlit as st
    bar = st.progress(0.0)

    def _cb(study, trial):       # callback pro Trial
        bar.progress((trial.number + 1) / n_trials)

    study.optimize(
        lambda tr: objective(tr, price_df, fixed_kwargs, search_space, kpi_weights),
        n_trials=n_trials,
        timeout=timeout,
        callbacks=[_cb],
        gc_after_trial=True,
        show_progress_bar=False,
    )
    bar.empty()
    return study
