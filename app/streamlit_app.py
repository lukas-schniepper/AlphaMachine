import streamlit as st
import json
from pathlib import Path
import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
import datetime as dt
from pandas.tseries.offsets import BDay
from pandas.io.formats.style import Styler
import tempfile
import os
from sqlmodel import select
import re
from typing import Dict, Optional, List, Any
import plotly.graph_objects as go
from AlphaMachine_core.models import TickerPeriod
from AlphaMachine_core.models import TickerPeriod, TickerInfo, PriceData
from AlphaMachine_core.db import init_db, get_session
from AlphaMachine_core.optimize_params import run_optimizer
from AlphaMachine_core.engine import SharpeBacktestEngine
from AlphaMachine_core.reporting_no_sparklines import export_results_to_excel
from AlphaMachine_core.data_manager import StockDataManager
from AlphaMachine_core.config import (
    OPTIMIZER_METHOD as CFG_OPT_METHOD,
    COV_ESTIMATOR as CFG_COV_EST,
    REBALANCE_FREQUENCY as CFG_REBAL_FREQ,
    CUSTOM_REBALANCE_MONTHS as CFG_CUSTOM_REBAL,
    ENABLE_TRADING_COSTS as CFG_ENABLE_TC,
    FIXED_COST_PER_TRADE as CFG_FIXED_COST,
    VARIABLE_COST_PCT as CFG_VAR_COST,
    BACKTEST_WINDOW_DAYS as CFG_WINDOW,
    OPTIMIZATION_MODE as CFG_OPT_MODE,
    MIN_WEIGHT as CFG_MIN_W,
    MAX_WEIGHT as CFG_MAX_W,
    FORCE_EQUAL_WEIGHT as CFG_FORCE_EQ,
)

# ---------- Fix A : RiskOverlay-Helper --------------------------------------
from AlphaMachine_core.risk_overlay.overlay import RiskOverlay
from AlphaMachine_core import config as CFG

@st.cache_resource
def get_overlay():
    """Overlay-Objekt gemäss globaler CFG laden (einmal pro Session)."""
    if CFG.RISK_OVERLAY.get("enabled", False):
        return RiskOverlay(CFG.RISK_OVERLAY["config_path"])
    return None

init_db() 

# ---------------------------------------------------------------
# Helper Funktion zur Formatierung von Zahlen
# ---------------------------------------------------------------
def fmt_df(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()

    def _fmt(col: str, val):
        if pd.isna(val):
            return ""
        if isinstance(val, (int, float, np.integer, np.floating)):
            pct_like = (
                "%" in col or
                any(tag in col.lower()
                    for tag in ["return", "drawdown", "weight",
                                "volatility", "cagr"])
            )
            if pct_like:
                return f"{val:.1f}%"
            else:
                return f"{val:,.0f}"
        return val

    for c in df2.columns:
        df2[c] = df2[c].apply(lambda v, col=c: _fmt(col, v))
    return df2 

# -----------------------------------------------------------------------------
# 1) Page-Config
# -----------------------------------------------------------------------------
st.set_page_config("AlphaMachine", layout="wide")

st.markdown(
    """
    <style>
    /*  GLOBAL: alle vertikalen & horizontalen Scrollbars breiter  */
    /*  WebKit-Browser (Chrome, Edge, Opera, Brave, …)            */
    ::-webkit-scrollbar {
        width: 18px;        /* vertikal */
        height: 18px;       /* horizontal */
    }
    ::-webkit-scrollbar-thumb {
        background: rgba(120, 120, 120, 0.6);
        border-radius: 8px;
        border: 3px solid rgba(0,0,0,0);   /* Abstand zum Rand (= “padding”) */
        background-clip: content-box;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(100, 100, 100, 0.9);
    }

    /*  Firefox (Quantum)  */
    html {
        scrollbar-width: thick;          /* thin | auto | <Länge> | none */
        scrollbar-color: rgba(120,120,120,0.6) transparent;
    }

    /*  Optional: nur DataFrames / AgGrid o. Ä. gezielt ansprechen
        (statt global) – Beispiel für die Streamlit-DataFrame-Box:
       [data-testid="stDataFrameScrollable"] {...}                       */
    </style>
    """,
    unsafe_allow_html=True
)

# -----------------------------------------------------------------------------
# 2) Passwort-Gate
# -----------------------------------------------------------------------------
pwd = st.sidebar.text_input("Passwort", type="password")
if pwd != st.secrets.get("APP_PW", ""):
    st.warning("🔒 Bitte korrektes Passwort eingeben.")
    st.stop()

# -----------------------------------------------------------------------------
# 3) Navigation-Switcher
# -----------------------------------------------------------------------------
page = st.sidebar.radio(
    "🗂️ Seite wählen",
    ["Backtester", "Optimizer", "Data Mgmt", "Risk Overlay Test"], # <--- NEUER EINTRAG
    index=0
)

# -----------------------------------------------------------------------------
# 4) CSV-Loader (Session-Cache)
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner="📂 CSV wird geladen…")
def load_csv(file):
    return pd.read_csv(file, index_col=0, parse_dates=True)


# -----------------------------------------------------------------------------
# Load Prices
# -----------------------------------------------------------------------------
def load_price_df(
    dm: StockDataManager, 
    tickers: List[str], 
    start_date: dt.date, # Annahme: start_date und end_date sind bereits date-Objekte
    end_date: dt.date, 
    window_days: int, 
    lookback_margin_days: int = 20
) -> tuple[pd.DataFrame, dt.date]: # Typ-Hinweis für Rückgabewert
    
    # Berechne das Lookback in HANDELSTAGEN!
    # pd.to_datetime ist hier nötig, falls start_date als dt.date kommt und BDay einen Timestamp erwartet
    price_start_dt = pd.to_datetime(start_date) - BDay(window_days + lookback_margin_days)
    price_start_for_query = price_start_dt.date()  # Konvertiere zu date für get_price_data

    # dm.get_price_data gibt jetzt eine Liste von Dictionaries zurück
    raw_price_dicts: List[Dict[str, Any]] = dm.get_price_data(
        tickers,
        price_start_for_query.strftime("%Y-%m-%d"), # Konvertiere zu String für die Methode
        end_date.strftime("%Y-%m-%d")               # Konvertiere zu String
    )

    if not raw_price_dicts:
        st.warning(f"Keine Rohdaten von get_price_data für Ticker {tickers} im Zeitraum {price_start_for_query} bis {end_date} erhalten.")
        return pd.DataFrame(), price_start_for_query # Gib leeren DF und das berechnete Startdatum zurück

    # Erstelle den DataFrame direkt aus der Liste von Dictionaries
    # Die Dictionaries sollten bereits die korrekten Keys haben: 
    # 'trade_date', 'ticker', 'open', 'high', 'low', 'close', 'volume'
    # wie von StockDataManager.get_price_data zurückgegeben
    try:
        temp_df = pd.DataFrame(raw_price_dicts)
    except Exception as e_df_create:
        st.error(f"Fehler beim Erstellen des DataFrames aus raw_price_dicts: {e_df_create}")
        return pd.DataFrame(), price_start_for_query

    if temp_df.empty:
        st.warning(f"Temporärer DataFrame ist leer nach Konvertierung von raw_price_dicts für Ticker {tickers}.")
        return pd.DataFrame(), price_start_for_query
        
    # Stelle sicher, dass die benötigten Spalten vorhanden sind
    if "trade_date" not in temp_df.columns or "ticker" not in temp_df.columns or "close" not in temp_df.columns:
        st.error(f"Benötigte Spalten ('trade_date', 'ticker', 'close') nicht im DataFrame aus get_price_data gefunden. Vorhandene Spalten: {temp_df.columns.tolist()}")
        return pd.DataFrame(), price_start_for_query

    try:
        price_df_pivoted = (
            temp_df
            .assign(date=lambda d: pd.to_datetime(d["trade_date"])) # Konvertiere 'trade_date' zu datetime
            .pivot(index="date", columns="ticker", values="close")  # Pivot für Close-Preise
            .sort_index()
        )
    except KeyError as ke:
        st.error(f"KeyError beim Pivotieren der Preisdaten (wahrscheinlich fehlende Spalte): {ke}. DataFrame Spalten: {temp_df.columns.tolist()}")
        return pd.DataFrame(), price_start_for_query
    except Exception as e_pivot:
        st.error(f"Allgemeiner Fehler beim Pivotieren der Preisdaten: {e_pivot}")
        return pd.DataFrame(), price_start_for_query

    # Erstelle einen vollständigen Business-Day-Index für den gesamten benötigten Zeitraum
    # price_start_dt ist der Timestamp für den allerersten Tag der benötigten Historie
    # end_date ist das Enddatum des Backtests
    full_idx = pd.date_range(start=price_start_dt.normalize(), end=pd.to_datetime(end_date).normalize(), freq='B') # 'B' für Business Days
    
    price_df_reindexed = price_df_pivoted.reindex(full_idx).ffill() # Vorwärtsfüllen für fehlende Tage

    if price_df_reindexed.empty:
        st.warning(f"Preis-DataFrame ist nach Reindizierung und ffill leer für Ticker {tickers}.")
    
    return price_df_reindexed, price_start_for_query


# =============================================================================
# === Backtester-UI ===
# =============================================================================
def show_backtester_ui():
    st.sidebar.header("📊 Backtest-Parameter")
    dm = StockDataManager()

    # 0) Backtest-Periode festlegen
    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input(
        "Backtest-Startdatum",
        value=dt.date.today() - dt.timedelta(days=5*365),
        max_value=dt.date.today()
    )
    end_date = col2.date_input(
        "Backtest-Enddatum",
        value=dt.date.today(),
        min_value=start_date
    )
    if start_date >= end_date:
        st.sidebar.error("Startdatum muss vor dem Enddatum liegen.")
        return

    # 1) Quellen-Auswahl (DB + Defaults)
    with get_session() as session:
        existing = session.exec(select(TickerPeriod.source)).all()
    defaults = ["Topweights","TR20"]
    sources = st.sidebar.multiselect(
        "Datenquellen auswählen",
        options=sorted(set(existing + defaults)),
        default=["Topweights"]
    )

    # 2) Monat wählen
    months = dm.get_periods_distinct_months()
    month  = st.sidebar.selectbox("Periode wählen (YYYY-MM)", months)

    # 3) Modus: statisch vs. dynamisch
    mode = st.sidebar.radio(
        "Ticker-Universe",
        ["statisch (gesamte Periode)", "dynamisch (monatlich)"]
    )

    # 4) Lookback Days (Backtest-Fenster)
    window_days = st.sidebar.slider(
        "Lookback Days", 
        min_value=50,
        max_value=500,
        value=CFG_WINDOW,
        step=10
    )

    # — 5) Portfolio- & Optimierungs-Parameter —
    start_balance = st.sidebar.number_input("Startkapital", 10_000, 1_000_000, 100_000, 1_000)
    num_stocks    = st.sidebar.slider("Aktien pro Portfolio", 5, 50, 20)
    opt_method    = st.sidebar.selectbox(
        "Optimierer", ["ledoit-wolf","minvar","hrp"],
        index=["ledoit-wolf","minvar","hrp"].index(CFG_OPT_METHOD)
    )
    cov_estimator = st.sidebar.selectbox(
        "Kovarianzschätzer", ["ledoit-wolf","constant-corr","factor-model"],
        index=["ledoit-wolf","constant-corr","factor-model"].index(CFG_COV_EST)
    )
    opt_mode      = st.sidebar.selectbox(
        "Optimierungsmodus", ["select-then-optimize","optimize-subset"],
        index=["select-then-optimize","optimize-subset"].index(CFG_OPT_MODE)
    )
    rebalance_freq= st.sidebar.selectbox(
        "Rebalance", ["weekly","monthly","custom"],
        index=["weekly","monthly","custom"].index(CFG_REBAL_FREQ)
    )
    custom_months = (
        st.sidebar.slider("Monate zwischen Rebalances", 1, 12, CFG_CUSTOM_REBAL)
        if rebalance_freq=="custom" else 1
    )

    # — 6) Gewicht-Constraints —
    min_w    = st.sidebar.slider("Min Weight (%)", 0.0, 5.0, CFG_MIN_W*100, 0.5) / 100.0
    max_w    = st.sidebar.slider("Max Weight (%)", 5.0, 50.0, CFG_MAX_W*100, 1.0) / 100.0
    force_eq = st.sidebar.checkbox("Force Equal Weight", CFG_FORCE_EQ)

    # — 7) Trading-Kosten —
    st.sidebar.subheader("Trading-Kosten")
    enable_tc  = st.sidebar.checkbox("Kosten aktiv", CFG_ENABLE_TC)
    fixed_cost = st.sidebar.number_input("Fixe Kosten pro Trade", 0.0, 100.0, CFG_FIXED_COST)
    var_cost   = st.sidebar.number_input("Variable Kosten (%)", 0.0, 1.0, CFG_VAR_COST*100) / 100.0

    
    # --- Risk-Overlay: feste Defaults, weil die Widgets entfernt wurden ------------
    overlay_enabled = False          # Overlay komplett aus
    low_thr  = -0.30                 # Default für Three-Band LOW
    high_thr =  0.10                 # Default für Three-Band HIGH

    
    # --- Buttons -----------------------------------------------
    run_btn     = st.sidebar.button("Backtest starten 🚀")

    # Wenn *keiner* gedrückt wurde → zurück
    if not run_btn:
        st.info("Stelle alle Parameter ein und klicke auf einen der Start‑Buttons.")
        return

    # — VALIDIERUNG —
    if not sources:
        st.error("Bitte mindestens eine Quelle auswählen.") 
        return
    if not month:
        st.error("Bitte einen Monat auswählen.")
        return

     # --- Ticker laden ---
    tickers = dm.get_tickers_for(month, sources)
    if not tickers:
        st.error("Keine Ticker für diese Auswahl.")
        return

    # --- Preisdaten laden ---
    price_df, price_start = load_price_df(dm, tickers, start_date, end_date, window_days)
    st.write(f"⏳ Lade Preisdaten von {price_start} bis {end_date}")
    st.write(f"Price-DF nach Load: {price_df.index.min()} bis {price_df.index.max()}")

    if price_df.empty:
        st.error("Keine Preisdaten gefunden.")
        return
    

    # ‣ wenn weniger Ticker da sind als num_stocks, auf available runterschrauben
    orig_num_stocks = num_stocks
    available = price_df.shape[1]
    if available < orig_num_stocks:
        st.warning(
            f"Achtung: nur {available} Aktien verfügbar; "
            f"Backtest wird mit {available} statt {orig_num_stocks} laufen"
        )
        num_stocks = available

    #DEBUG
    #st.write("🔎 price_df shape:", price_df.shape)
    #st.write(price_df.head())
    #----------------------

    with st.spinner("📈 Backtest läuft…"):
        # ------------------------------------------
        # Overlay-Konfiguration *vor* dem Backtest
        # ------------------------------------------
        overlay_cfg_path = Path(CFG.RISK_OVERLAY["config_path"])
        cfg = json.loads(overlay_cfg_path.read_text())

        # ► Mapping-Schwellen übernehmen
        cfg["mapping"]["params"].update({"low": low_thr, "high": high_thr})
        overlay_cfg_path.write_text(json.dumps(cfg, indent=2))

        # -------------------------------------------------
        # 1) BASELINE – **ohne** Risk-Overlay
        # -------------------------------------------------
        engine_baseline = SharpeBacktestEngine(
            price_df,
            start_balance,
            num_stocks,
            start_month=start_date.strftime("%Y-%m-%d"),
            universe_mode="static" if mode.startswith("statisch") else "dynamic",
            optimizer_method=opt_method,
            cov_estimator=cov_estimator,
            rebalance_frequency=rebalance_freq,
            custom_rebalance_months=custom_months,
            window_days=window_days,
            min_weight=min_w,
            max_weight=max_w,
            force_equal_weight=force_eq,
            enable_trading_costs=enable_tc,
            fixed_cost_per_trade=fixed_cost,
            variable_cost_pct=var_cost,
            optimization_mode=opt_mode,
            use_risk_overlay=False,          # <<—— einzig relevante Änderung
        )
        engine_baseline.run_with_next_month_allocation()

        # -------------------------------------------------
        # 2) OVERLAY – Risk-On / Risk-Off (optional)
        # -------------------------------------------------
        engine_overlay = SharpeBacktestEngine(
            price_df,
            start_balance,
            num_stocks,
            start_month=start_date.strftime("%Y-%m-%d"),
            universe_mode="static" if mode.startswith("statisch") else "dynamic",
            optimizer_method=opt_method,
            cov_estimator=cov_estimator,
            rebalance_frequency=rebalance_freq,
            custom_rebalance_months=custom_months,
            window_days=window_days,
            min_weight=min_w,
            max_weight=max_w,
            force_equal_weight=force_eq,
            enable_trading_costs=enable_tc,
            fixed_cost_per_trade=fixed_cost,
            variable_cost_pct=var_cost,
            optimization_mode=opt_mode,
            use_risk_overlay=overlay_enabled,  # <<—— nur aktiv, wenn Checkbox gesetzt
        )
        engine_overlay.run_with_next_month_allocation()

        # -------------------------------------------------
        # 3) Parameter-Dict für den UI-Tab
        # -------------------------------------------------
        ui_params = {
            "Backtest Startdatum": start_date.strftime("%Y-%m-%d"),
            "Backtest Enddatum":   end_date.strftime("%Y-%m-%d"),
            "Quellen":             ", ".join(sources),
            "Periode (YYYY-MM)":    month,
            "Ticker-Universe":      mode,
            "Lookback Days":        window_days,
            "Startkapital":         start_balance,
            "Aktien pro Portfolio": num_stocks,
            "Optimierer":           opt_method,
            "Kovarianzschätzer":    cov_estimator,
            "Optimierungsmodus":    opt_mode,
            "Rebalance":            rebalance_freq,
            "Custom Monate":        custom_months if rebalance_freq == "custom" else "-",
            "Min Weight (%)":       round(min_w * 100, 2),
            "Max Weight (%)":       round(max_w * 100, 2),
            "Force Equal Weight":   force_eq,
            "Trading-Kosten aktiv": enable_tc,
            "Fixe Kosten/Trade":    fixed_cost,
            "Variable Kosten (%)":  round(var_cost * 100, 2),
            "Risk-Overlay aktiv":   overlay_enabled,      # <<—— zum schnellen Nachvollziehen
            "Three-Band Low":       low_thr,
            "Three-Band High":      high_thr,
        }

    # -----------------------------------------------------
    msg = "Backtest fertig ✅"
    if available < orig_num_stocks:
        msg += f"  (Achtung: nur {available} Stocks vorhanden statt {orig_num_stocks})"
    st.success(msg)


    # Tabs
    tabs = st.tabs([
        "Dashboard",
        "Overlay",
        "Daily",
        "Monthly",
        "Yearly",
        "Monthly Allocation",
        "Next Month Allocation",
        "Drawdowns",
        "Trading Costs",
        "Rebalance",
        "Paramter",
        "Logs"
    ])

    # ────────────────────────────────────────────────────────────────
    # Tabs-Block (1:1 ersetzbar)
    # ────────────────────────────────────────────────────────────────
    with tabs[0]:  # Dashboard
        st.subheader("🔍 KPIs")

        # KPI-Tabelle als statische Tabelle (st.table ⇒ Styling bleibt)
        st.table(fmt_df(engine_baseline.performance_metrics))

        st.markdown("---")
        st.subheader("📈 Portfolio-Verlauf")
        st.line_chart(
            pd.DataFrame({"Baseline": engine_baseline.portfolio_value})
        )

        st.markdown("---")
        st.subheader("📆 Monatliche Performance (%)")
        perf_df = pd.DataFrame({
            "Baseline": engine_baseline.monthly_performance
                        .set_index("Date")["Monthly PnL (%)"]
        })
        st.bar_chart(perf_df)


    with tabs[1]:  # Overlay
        st.subheader("⚖️ Risk-On / Risk-Off Overlay")
        overlay_obj = engine_overlay.risk_overlay
        if overlay_obj is None:
            st.info("Overlay ist in der config deaktiviert.")
        elif not overlay_obj.score_log:
            st.info("Noch keine Scores berechnet – starte zuerst den Backtest.")
        else:
            df_score = (pd.DataFrame(overlay_obj.score_log)
                        .set_index("date")
                        .rename(columns={"score": "Aggregated Score"}))
            st.line_chart(df_score, height=200)

            last_score  = df_score["Aggregated Score"].iloc[-1]
            last_weight = overlay_obj.map_to_equity_weight(last_score)
            st.markdown(
                f"**Aktuelle Ziel-Aktienquote:** {last_weight:.0%}  "
                f"(Score {last_score:+.2f})"
            )


    with tabs[2]:  # Daily
        st.subheader("📅 Daily Portfolio Baseline")
        df_daily = engine_baseline.daily_df.copy()

        if isinstance(df_daily.index, pd.DatetimeIndex):
            df_daily.index = df_daily.index.date
        elif "Date" in df_daily.columns:
            # Falls Datum als Spalte existiert (selten bei Daily-DFs)
            df_daily["Date"] = pd.to_datetime(df_daily["Date"]).dt.date

        st.dataframe(fmt_df(df_daily), use_container_width=True)


    with tabs[3]:  # Monthly
        st.subheader("🗓️ Monthly Performance Baseline")
        if not engine_baseline.monthly_performance.empty:
            df_mon = engine_baseline.monthly_performance.copy()
            df_mon["Date"] = (pd.to_datetime(df_mon["Date"])
                            .dt.strftime("%Y - %b"))  # «2025 - Jan»
            st.dataframe(fmt_df(df_mon), use_container_width=True)
        else:
            st.info("Keine Monatsdaten für Baseline.")


    with tabs[4]:  # Yearly
        st.subheader("🗓️ Yearly Performance Baseline")
        if not engine_baseline.portfolio_value.empty:
            yearly_balance = engine_baseline.portfolio_value.resample("YE").last()
            yearly_start   = engine_baseline.portfolio_value.resample("YE").first()
            yearly_pnl     = yearly_balance - yearly_start
            yearly_ret     = yearly_balance.pct_change()*100

            monthly_pnl = engine_baseline.monthly_performance.copy()
            monthly_pnl["Year"] = pd.to_datetime(monthly_pnl["Date"]).dt.year
            yearly_monthly_pnl = monthly_pnl.groupby("Year")["Monthly PnL ($)"].sum()

            df_year = pd.DataFrame({
                "Year": yearly_balance.index.year.astype(str),  # als Text ⇒ kein «2,025»
                "Yearly PnL (Portfolio Value)": yearly_pnl.values,
                "Yearly PnL (Sum Monthly)": yearly_monthly_pnl
                                            .reindex(yearly_balance.index.year)
                                            .values,
                "Return (%)": yearly_ret.values,
                "Balance": yearly_balance.values
            }).reset_index(drop=True)

            st.dataframe(fmt_df(df_year), use_container_width=True)
        else:
            st.info("Keine Jahresdaten für Baseline.")


    with tabs[5]:  # Monthly Allocation
        st.subheader("📊 Monthly Allocation Baseline")
        if not engine_baseline.monthly_allocations.empty:
            df_sorted = engine_baseline.monthly_allocations.sort_values(
                by="Rebalance Date", ascending=False
            )
            st.dataframe(fmt_df(df_sorted), use_container_width=True)
        else:
            st.info("Keine Daten für Baseline.")


    with tabs[6]:  # Next-Month Allocation
        st.subheader("🔮 Next Month Allocation Baseline")
        if hasattr(engine_baseline, "next_month_weights"):
            df_next = (engine_baseline.next_month_weights
                    .mul(100)
                    .reset_index())
            df_next.columns = ["Ticker", "Gewicht (%)"]
            st.dataframe(fmt_df(df_next), use_container_width=True)
        else:
            st.info("Keine Auswahl für den Folgemonat (Baseline).")


    with tabs[7]:  # Drawdowns
        st.subheader("📉 Top 10 Drawdowns Baseline")
        df_port = engine_baseline.portfolio_value.to_frame(name="Portfolio")
        df_port["Peak"] = df_port["Portfolio"].cummax()
        df_port["Drawdown"] = df_port["Portfolio"] / df_port["Peak"] - 1

        periods, in_dd = [], False
        for date, row in df_port.iterrows():
            if not in_dd and row["Drawdown"] < 0:
                in_dd, start = True, date
                peak_val = row["Peak"]; trough_val = row["Portfolio"]; trough = date
            elif in_dd:
                if row["Portfolio"] < trough_val:
                    trough_val, trough = row["Portfolio"], date
                if row["Portfolio"] >= peak_val:
                    periods.append({
                        "Start":         start.date(),
                        "Trough":        trough.date(),
                        "End":           date.date(),
                        "Length (Days)": (date-start).days,
                        "Recovery Time": (date-trough).days,
                        "Drawdown (%)":  round((trough_val/peak_val-1)*100, 2),
                    })
                    in_dd = False
        if in_dd:  # offenes DD-Ende
            last_date = df_port.index[-1]
            periods.append({
                "Start":         start.date(),
                "Trough":        trough.date(),
                "End":           last_date.date(),
                "Length (Days)": (last_date-start).days,
                "Recovery Time": None,
                "Drawdown (%)":  round((trough_val/peak_val-1)*100, 2),
            })

        df_dd = pd.DataFrame(periods)
        if "Drawdown (%)" in df_dd.columns and not df_dd.empty:
            df_dd = (df_dd.sort_values(by="Drawdown (%)")
                        .head(10)
                        .reset_index(drop=True))
            st.dataframe(fmt_df(df_dd), use_container_width=True)
        else:
            st.info("Keine Drawdown-Daten für dieses Portfolio.")


    # Tab 5: Trading Costs
    with tabs[8]:
        st.subheader("💸 Trading Costs Baseline")
        if (not engine_baseline.monthly_allocations.empty and
            "Trading Costs" in engine_baseline.monthly_allocations):
            cost_df = (engine_baseline.monthly_allocations
                    .dropna(subset=["Trading Costs"])
                    .groupby("Rebalance Date")["Trading Costs"]
                    .sum()
                    .reset_index(name="Total Trading Costs"))
            cost_df["Rebalance Date"] = pd.to_datetime(cost_df["Rebalance Date"]).dt.date
            st.dataframe(fmt_df(cost_df), use_container_width=True)
        else:
            st.info("Keine Trading-Kosten-Daten für Baseline.")


    # Tab 6: Rebalance Analysis
    with tabs[9]:
        st.subheader("🔁 Rebalance Analysis")
        if (hasattr(engine_baseline, 'selection_details')
            and engine_baseline.selection_details):
            try:
                df_rebalance = pd.DataFrame(engine_baseline.selection_details)
                if "Rebalance Date" in df_rebalance.columns:
                    df_rebalance = df_rebalance[
                        df_rebalance["Rebalance Date"] != "SUMMARY"
                    ].copy()

                if not df_rebalance.empty:
                    if "Rebalance Date" in df_rebalance.columns:
                        df_rebalance.loc[:, "Rebalance Date"] = (
                            pd.to_datetime(df_rebalance["Rebalance Date"],
                                        errors='coerce').dt.date  # Zeit weg
                        )
                    st.dataframe(fmt_df(df_rebalance), use_container_width=True)
                else:
                    st.info("Keine Rebalance-Detaildaten für die Anzeige.")
            except Exception as e:
                st.error(f"Fehler beim Anzeigen der Rebalance-Details: {e}")
                st.write("Struktur von selection_details:",
                        engine_baseline.selection_details[:2])
        else:
            st.info("Keine Rebalance-Detaildaten vorhanden.")


    # Tab 7: Parameters
    with tabs[10]:
        st.subheader("⚙️ Ausgewählte Backtest-Parameter")
        df_params = pd.DataFrame(ui_params.items(),
                                columns=["Parameter", "Wert"])
        df_params["Wert"] = df_params["Wert"].astype(str)
        st.dataframe(fmt_df(df_params), use_container_width=True)


    # Tab 8: Logs
    with tabs[11]:
        st.subheader("🪵 Logs")
        for line in (engine_baseline.ticker_coverage_logs +
                    engine_baseline.log_lines):
            st.text(line)


    # Excel Download: Baseline und Overlay getrennt
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Baseline-Report
        path_baseline = os.path.join(tmp_dir, f"AlphaMachine_Baseline_{dt.date.today()}.xlsx")
        export_results_to_excel(engine_baseline, path_baseline)
        with open(path_baseline, "rb") as f:
            st.download_button(
                "📥 Excel-Report Baseline",
                f.read(),
                file_name=os.path.basename(path_baseline),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    

# =============================================================================
# === Data-Management-UI ===
# =============================================================================
def show_data_ui():
    st.header("📂 Data Management")

    if 'sdm_instance' not in st.session_state:
        try:
            st.session_state.sdm_instance = StockDataManager()
        except Exception as e:
            st.error(f"Fehler Initialisierung StockDataManager: {e}"); st.exception(e); st.stop()
    dm = st.session_state.sdm_instance

    mode = st.radio("Modus", ["➕ Add/Update", "👁️ View/Delete"], index=0, key="data_ui_mode_radio_main_v2")

    if mode == "➕ Add/Update":
        st.subheader("➕ Ticker einfügen & Daten updaten")
        tickers_text = st.text_area("Tickers (eine pro Zeile)", height=120, key="data_ui_tickers_text_main_v2")
        col_date, col_source = st.columns(2)
        month_dt_input = col_date.date_input("Monat wählen", value=dt.date.today().replace(day=1), key="data_ui_month_date_main_v2")
        start_period = month_dt_input.replace(day=1)
        end_period = (pd.to_datetime(start_period) + pd.offsets.MonthEnd(0)).date()
        col_date.write(f"Zeitraum: {start_period} bis {end_period}")
        
        existing_sources_db = []
        try:
            with get_session() as session: 
                results = session.exec(select(TickerPeriod.source).distinct()).all()
                existing_sources_db = [str(s) for s in results if s] 
        except Exception as e_es: st.error(f"Fehler Laden existierender Quellen: {e_es}")
        
        default_sources = ["Topweights", "Manual"]
        source_options = sorted(list(set(existing_sources_db + default_sources))); source_options.append("Andere…")
        
        source_selected_add = col_source.selectbox("Quelle", source_options, key="data_ui_source_select_main_v2")
        if source_selected_add == "Andere…":
            custom_source_add = col_source.text_input("Neue Quelle eingeben", key="data_ui_custom_source_main_v2")
            if custom_source_add: source_selected_add = custom_source_add
        
        if st.button("➕ Ticker zu Periode hinzufügen", key="data_ui_add_button_main_v2"):
            tickers_list = [t.strip().upper() for t in tickers_text.splitlines() if t.strip()]
            if tickers_list and source_selected_add not in ["Andere…", ""]:
                added = dm.add_tickers_for_period(tickers_list, start_period.strftime("%Y-%m-%d"), end_period.strftime("%Y-%m-%d"), source_selected_add)
                st.success(f"{len(added)} Ticker zur Periode hinzugefügt ({source_selected_add}, {start_period.strftime('%Y-%m')}).")
                if 'all_periods_for_filters_cached_data_ui' in st.session_state: del st.session_state['all_periods_for_filters_cached_data_ui']
                st.rerun() 
            else: st.warning("Bitte Ticker und eine gültige Quelle eingeben.")

        if st.button("🔄 Alle Preise für DB-Ticker updaten", key="data_ui_update_button_main_v2"):
            tickers_in_db_update: List[str] = []
            try:
                with get_session() as session: 
                    results = session.exec(select(TickerPeriod.ticker).distinct()).all()
                    tickers_in_db_update = sorted(list(set(str(t) for t in results if t)))
            except Exception as e_gt_db: st.error(f"Fehler Holen Ticker für Update: {e_gt_db}")
            
            if not tickers_in_db_update: st.info("Keine Ticker in DB zum Updaten.")
            else:
                st.info(f"Update für {len(tickers_in_db_update)} Ticker startet..."); progress_bar = st.progress(0.0); status_text = st.empty(); updated_count = 0
                for idx, tk_str in enumerate(tickers_in_db_update):
                    status_text.info(f"📡 Lade Preise für {tk_str} ({idx+1}/{len(tickers_in_db_update)})...")
                    success_list = dm.update_ticker_data(tickers=[tk_str]); 
                    if success_list: updated_count += 1
                    progress_bar.progress((idx + 1) / len(tickers_in_db_update))
                status_text.success(f"✅ Preisupdate abgeschlossen. {updated_count} von {len(tickers_in_db_update)} Ticker hatten neue Daten/wurden aktualisiert.")
        return 

    # --- View/Delete Mode ---
    st.subheader("👁️ View/Delete Ticker Periods")
    all_periods_for_filters: List[Dict[str, Any]]
    if 'all_periods_for_filters_cached_data_ui' not in st.session_state:
        with st.spinner("Lade Periodendaten für Filter..."):
            temp_all_periods_for_filters_calc = []
            try:
                with get_session() as session: 
                    db_objects = session.exec(select(TickerPeriod).order_by(TickerPeriod.start_date.desc(), TickerPeriod.source, TickerPeriod.ticker)).all()
                    temp_all_periods_for_filters_calc = [p.model_dump() for p in db_objects]
                st.session_state.all_periods_for_filters_cached_data_ui = temp_all_periods_for_filters_calc
            except Exception as e_load_all_p:
                st.error(f"Fehler beim Laden aller Perioden für Filter: {e_load_all_p}")
                st.session_state.all_periods_for_filters_cached_data_ui = []
    all_periods_for_filters = st.session_state.get('all_periods_for_filters_cached_data_ui', [])
    temp_monate_set = set(); temp_quellen_set = set()
    if not all_periods_for_filters: st.info("Keine TickerPeriod-Einträge."); Monate, Quellen = ["(Keine)"], ["(Keine)"]
    else:
        for p_dict in all_periods_for_filters:
            start_date_val = p_dict.get('start_date')
            if start_date_val:
                current_date_obj = None
                if isinstance(start_date_val, str): 
                    try: current_date_obj = pd.to_datetime(start_date_val).date()
                    except: continue 
                elif isinstance(start_date_val, dt.date):
                    current_date_obj = start_date_val
                
                if current_date_obj: temp_monate_set.add(current_date_obj.strftime("%Y-%m"))

            if p_dict.get('source'): temp_quellen_set.add(str(p_dict['source']))
        
        Monate  = sorted(list(temp_monate_set), reverse=True) if temp_monate_set else ["(Keine)"]
        Quellen = sorted(list(temp_quellen_set)) if temp_quellen_set else ["(Keine)"]
        
    col_v_month, col_v_source = st.columns(2)
    month_selected_view = col_v_month.selectbox("Monat anzeigen/filtern", Monate, key="view_del_month_sb_page_v5")
    source_selected_view = col_v_source.selectbox("Quelle anzeigen/filtern", Quellen, key="view_del_source_sb_page_v5")
    
    periods_to_display_view: List[Dict[str, Any]] = []
    if month_selected_view not in ["(Keine)", "(Fehler)"] and source_selected_view not in ["(Keine)", "(Fehler)"]:
        periods_to_display_view = [
            p_dict for p_dict in all_periods_for_filters 
            if (p_dict.get('start_date') and 
                ((isinstance(p_dict['start_date'], dt.date) and p_dict['start_date'].strftime("%Y-%m") == month_selected_view) or 
                 (isinstance(p_dict['start_date'], str) and p_dict['start_date'].startswith(month_selected_view))))
            and p_dict.get('source') == source_selected_view
        ]

    if periods_to_display_view:
        dfp_view = pd.DataFrame(periods_to_display_view)
        display_cols_view = ['id', 'ticker', 'start_date', 'end_date', 'source']
        dfp_display_view = dfp_view[[col for col in display_cols_view if col in dfp_view.columns]]
        if 'id' in dfp_display_view.columns:
            st.dataframe(dfp_display_view.set_index('id'), use_container_width=True, height=min(300, len(dfp_display_view)*38 + 58))
            available_ids_del_view = dfp_display_view['id'].tolist()
            if available_ids_del_view:
                to_del_view = st.multiselect("Perioden zum Löschen (IDs)", available_ids_del_view, key="del_ms_data_ui_page_v5")
                if st.button("🗑️ Ausgewählte löschen", key="del_btn_data_ui_page_v5"):
                    deleted_count = 0
                    for pid_to_delete in to_del_view:
                        if dm.delete_period(int(pid_to_delete)): deleted_count += 1
                    st.success(f"{deleted_count} von {len(to_del_view)} Einträgen gelöscht.")
                    if 'all_periods_for_filters_cached_data_ui' in st.session_state: del st.session_state['all_periods_for_filters_cached_data_ui']
                    st.rerun()
            else: st.info("Keine IDs zum Löschen.")
        else: st.warning("DFP hat keine 'id'-Spalte."); st.dataframe(dfp_display_view, use_container_width=True)
    elif month_selected_view not in ["(Keine)", "(Fehler)"]: st.info(f"Keine Period-Einträge für '{month_selected_view}' / '{source_selected_view}'.")

    st.markdown("---"); st.subheader("Ticker Info Übersicht")
    info_dicts_ui: List[Dict[str, Any]] = []; dfi_for_chart = pd.DataFrame() 
    try: info_dicts_ui = dm.get_ticker_info()
    except Exception as e_get_ti_ui: st.error(f"Fehler Laden TickerInfo: {e_get_ti_ui}")
    if not info_dicts_ui: st.info("Keine TickerInfo vorhanden.")
    else:
        dfi_for_chart = pd.DataFrame(info_dicts_ui) 
        if 'id' not in dfi_for_chart.columns: st.error("Spalte 'id' fehlt in TickerInfo."); st.dataframe(dfi_for_chart, use_container_width=True)
        else:
            all_cols_dfi_view = list(dfi_for_chart.columns)
            filter_col_dfi_view = st.selectbox("Filter-Spalte TickerInfo", ["(kein)"] + all_cols_dfi_view, index=0, key="ti_filter_col_data_ui_v5")
            
            df_to_display_ticker_info = dfi_for_chart.copy() # Für die Anzeige, Original für Chart-Ticker-Liste behalten
            if filter_col_dfi_view != "(kein)" and filter_col_dfi_view in df_to_display_ticker_info.columns:
                unique_choices_dfi_view = sorted(df_to_display_ticker_info[filter_col_dfi_view].dropna().astype(str).unique())
                if unique_choices_dfi_view:
                    sel_dfi_view = st.multiselect(f"Werte «{filter_col_dfi_view}»", unique_choices_dfi_view, default=unique_choices_dfi_view, key="ti_ms_data_ui_v5")
                    df_to_display_ticker_info = df_to_display_ticker_info[df_to_display_ticker_info[filter_col_dfi_view].astype(str).isin(sel_dfi_view)]
                else: st.info(f"Keine Werte in '{filter_col_dfi_view}' zum Filtern.")
            
            if 'id' in df_to_display_ticker_info.columns:
                st.dataframe(df_to_display_ticker_info.set_index("id"), use_container_width=True, height=min(300, len(df_to_display_ticker_info)*38 + 58))
            elif not df_to_display_ticker_info.empty :
                 st.dataframe(df_to_display_ticker_info, use_container_width=True, height=min(300, len(df_to_display_ticker_info)*38 + 58))
            else: # df_to_display_ticker_info ist leer (z.B. nach Filterung)
                if filter_col_dfi_view != "(kein)": st.info("Keine TickerInfo-Daten nach Filterung.")
                # Ansonsten wurde schon "Keine TickerInfo vorhanden" angezeigt, wenn info_dicts_ui leer war


    # --- Price Chart ---
    st.markdown("---"); st.subheader("📈 Price Chart (Linienchart, max. letzte 10 Jahre)") # Titel angepasst
    available_tickers_pc_list = []
    # dfi_for_chart sollte aus dem vorherigen "Ticker Info Übersicht"-Block korrekt initialisiert sein
    if 'dfi_for_chart' in locals() and isinstance(dfi_for_chart, pd.DataFrame) and not dfi_for_chart.empty and 'ticker' in dfi_for_chart.columns:
        available_tickers_pc_list = sorted(dfi_for_chart["ticker"].dropna().unique())
    
    if not available_tickers_pc_list:
        st.info("Keine Ticker für Chart verfügbar (aus TickerInfo).")
    else:
        if 'chart_ticker_sel_data_ui_v7' not in st.session_state or \
           st.session_state.chart_ticker_sel_data_ui_v7 not in available_tickers_pc_list:
            st.session_state.chart_ticker_sel_data_ui_v7 = available_tickers_pc_list[0]

        def reset_chart_dates_on_ticker_change_v2(): # Neuer Name für Eindeutigkeit
            new_ticker = st.session_state.sb_chart_ticker_key_v7 
            st.session_state.chart_start_date_key_v7 = f"chart_start_{new_ticker}"
            st.session_state.chart_end_date_key_v7 = f"chart_end_{new_ticker}"
            
            _max_date_limit_cb = dt.date.today()
            # *** ÄNDERUNG: Default Start auf max. 10 Jahre in der Vergangenheit ***
            _min_date_for_chart_default = _max_date_limit_cb - pd.DateOffset(years=10)
            _min_date_limit_cb = dt.date(1990,1,1) # Globales Minimum bleibt

            _def_start_cb = _min_date_for_chart_default.date()
            _def_end_cb = _max_date_limit_cb
            
            if 'dfi_for_chart' in locals() and isinstance(dfi_for_chart, pd.DataFrame) and not dfi_for_chart.empty:
                _info_df_cb = dfi_for_chart[dfi_for_chart['ticker'] == new_ticker]
                if not _info_df_cb.empty:
                    _s_val_cb = _info_df_cb["actual_start_date"].min()
                    _e_val_cb = _info_df_cb["actual_end_date"].max()
                    if pd.notna(_s_val_cb): 
                        # Nimm das spätere von (Ticker-Start oder Default-10-Jahre-Start)
                        _def_start_cb = max(pd.to_datetime(_s_val_cb).date(), _min_date_for_chart_default.date())
                    if pd.notna(_e_val_cb): 
                        _def_end_cb = min(pd.to_datetime(_e_val_cb).date(), _max_date_limit_cb)
            
            if _def_start_cb > _def_end_cb: 
                _def_start_cb = max(_min_date_limit_cb, _def_end_cb - pd.Timedelta(days=1))
            
            st.session_state[st.session_state.chart_start_date_key_v7] = _def_start_cb
            st.session_state[st.session_state.chart_end_date_key_v7] = _def_end_cb
            
            cache_key_to_delete = f"chart_df_{new_ticker}_{_def_start_cb}_{_def_end_cb}"
            if cache_key_to_delete in st.session_state:
                del st.session_state[cache_key_to_delete]
        
        ticker_sel_for_chart = st.selectbox(
            "Ticker für Chart", 
            available_tickers_pc_list, 
            index=available_tickers_pc_list.index(st.session_state.chart_ticker_sel_data_ui_v7),
            key="sb_chart_ticker_key_v7",
            on_change=reset_chart_dates_on_ticker_change_v2
        )

        start_date_widget_key = st.session_state.get('chart_start_date_key_v7', f"chart_start_{ticker_sel_for_chart}")
        end_date_widget_key = st.session_state.get('chart_end_date_key_v7', f"chart_end_{ticker_sel_for_chart}")

        if start_date_widget_key not in st.session_state or end_date_widget_key not in st.session_state:
            reset_chart_dates_on_ticker_change_v2() 
            start_date_widget_key = st.session_state.chart_start_date_key_v7
            end_date_widget_key = st.session_state.chart_end_date_key_v7

        val_start = st.session_state[start_date_widget_key]
        val_end = st.session_state[end_date_widget_key]

        col_chart_date_start, col_chart_date_end = st.columns(2)
        # *** ÄNDERUNG: min_value für Startdatum auf 10 Jahre vor Enddatum begrenzen ***
        min_selectable_start_date = val_end - pd.DateOffset(years=10)
        min_selectable_start_date = max(dt.date(1990,1,1), min_selectable_start_date.date())


        selected_start_date = col_chart_date_start.date_input("Startdatum Chart", 
                                                               value=val_start, 
                                                               min_value=min_selectable_start_date, # Begrenzt Auswahl
                                                               max_value=val_end, # Kann nicht nach Enddatum sein
                                                               key=f"di_start_v7_{ticker_sel_for_chart}")
        selected_end_date = col_chart_date_end.date_input("Enddatum Chart", 
                                                          value=val_end, 
                                                          min_value=selected_start_date, # Muss nach Startdatum sein
                                                          max_value=dt.date.today(), 
                                                          key=f"di_end_v7_{ticker_sel_for_chart}")

        if selected_start_date != val_start or selected_end_date != val_end:
            st.session_state[start_date_widget_key] = selected_start_date
            st.session_state[end_date_widget_key] = selected_end_date
            # Invaliere Cache, indem du den spezifischen Chart-Cache-Key löschst
            # (Die Keys im Cache werden mit den *neuen* Daten gebildet, daher wird alter Cache nicht getroffen)
            st.rerun() 

        final_start_for_query = st.session_state[start_date_widget_key]
        final_end_for_query = st.session_state[end_date_widget_key]
        
        @st.cache_data(ttl=300) 
        def get_and_convert_chart_data_line(_ticker_arg, _start_date_str_arg, _end_date_str_arg):
            _sdm = st.session_state.sdm_instance
            if _sdm is None: return pd.DataFrame()
            _raw_dicts = _sdm.get_price_data([_ticker_arg], _start_date_str_arg, _end_date_str_arg)
            if not _raw_dicts: return pd.DataFrame()
            _chart_df = convert_single_ticker_pricedata_to_ohlcv_df(
                _raw_dicts, _ticker_arg, 
                pd.to_datetime(_start_date_str_arg), pd.to_datetime(_end_date_str_arg)
            )
            return _chart_df # Gibt immer noch den OHLCV-DataFrame zurück

        chart_df_to_display = get_and_convert_chart_data_line(ticker_sel_for_chart, 
                                                       final_start_for_query.strftime("%Y-%m-%d"), 
                                                       final_end_for_query.strftime("%Y-%m-%d"))

        if chart_df_to_display.empty or 'Close' not in chart_df_to_display.columns or chart_df_to_display['Close'].isna().all():
            st.info(f"Keine gültigen Schlusskursdaten für {ticker_sel_for_chart} im Zeitraum {final_start_for_query} bis {final_end_for_query} gefunden.")
        else:
            # *** ÄNDERUNG: Line Chart für Schlusskurse ***
            fig_price_chart = go.Figure()
            fig_price_chart.add_trace(go.Scatter(
                x=chart_df_to_display.index, 
                y=chart_df_to_display["Close"], 
                mode='lines', 
                name=f"{ticker_sel_for_chart} Close"
            ))
            fig_price_chart.update_layout(
                title=f"Linienchart (Schlusskurs) für {ticker_sel_for_chart}",
                xaxis_title="Date",
                yaxis_title="Price"
            )
            st.plotly_chart(fig_price_chart, use_container_width=True)

            # Fehlende Tage Logik (bleibt, da sie auf dem Index basiert)
            st.markdown("---") 
            # Statt Subheader, vielleicht ein Expander, wenn es nicht immer relevant ist
            with st.expander("Analyse fehlender Handelstage im Chart-Zeitraum", expanded=False):
                try:
                    us_bd_cal = CustomBusinessDay(calendar=USFederalHolidayCalendar())
                    full_bday_range = pd.date_range(final_start_for_query, final_end_for_query, freq=us_bd_cal)
                    # Verwende den Index des DataFrames, der tatsächlich geplottet wird
                    missing_bdays = full_bday_range.difference(chart_df_to_display.dropna(subset=['Close']).index) 
                    
                    if not missing_bdays.empty:
                        st.warning(f"⚠️ {len(missing_bdays)} Handelstage ohne Daten ({final_start_for_query.strftime('%Y-%m-%d')} bis {final_end_for_query.strftime('%Y-%m-%d')}):")
                        st.write(missing_bdays.strftime("%Y-%m-%d").tolist()[:20]) 
                    else:
                        st.success(f"Keine fehlenden Handelstage im Zeitraum ({final_start_for_query.strftime('%Y-%m-%d')} bis {final_end_for_query.strftime('%Y-%m-%d')}) gefunden.")
                except Exception as e_mdays:
                    st.warning(f"Fehler bei Prüfung fehlender Handelstage: {e_mdays}")

# -----------------------------------------------------------------------------
# Optimizer
# -----------------------------------------------------------------------------
def show_optimizer_ui():
    st.header("⚙️ Hyperparameter-Optimizer")

    # ---------- Daten-Selektion ------------------------------------
    dm = StockDataManager()
    month   = st.selectbox("Start-Monat (Universe)", dm.get_periods_distinct_months())
    with get_session() as session:
        existing = session.exec(select(TickerPeriod.source)).all()
    defaults = ["Topweights", "TR20"]
    sources  = st.multiselect("Quellen", sorted(set(existing + defaults)), default=defaults)
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Backtest-Start", value=dt.date.today() - dt.timedelta(days=5*365))
    end_date   = col2.date_input("Backtest-Ende",  value=dt.date.today(), min_value=start_date)

    # Preise laden
    # ----- Preise laden -----------------------------------------
    tickers = dm.get_tickers_for(month, sources)
    if not tickers:
        st.warning("⚠️ Keine Ticker für diese Auswahl.")
        st.stop()

    MAX_LOOKBACK = 1000  # größtes window_days im Suchraum
    price_df, price_start = load_price_df(
        dm, tickers, start_date, end_date, MAX_LOOKBACK
    )

    # ---------- Suchraum-Editor ------------------------------------
    PARAMS = {
        "num_stocks":        ("Anzahl Aktien", 5, 50, 1),
        "window_days":       ("Lookback Tage", 50, 500, 10),
        "min_weight":        ("Min-Weight %", 0.0, 5.0, 0.5),
        "max_weight":        ("Max-Weight %", 5.0, 50.0, 1.0),
        "force_equal_weight":("Equal-Weight", [False, True]),
        "optimization_mode": ("Mode", ["select-then-optimize", "optimize-subset"]),
        "optimizer_method":  ("Optimizer", ["ledoit-wolf", "minvar", "hrp"]),
        "cov_estimator":     ("Cov-Estimator", ["ledoit-wolf", "constant-corr", "factor-model"]),
    }

    search_space = {}
    with st.expander("🔧 Suchraum definieren", expanded=True):
        for key, meta in PARAMS.items():
            label = meta[0]
            if not st.checkbox(f"{label} optimieren", key=f"chk_{key}"):
                continue

            if isinstance(meta[1], (int, float)):
                lo, hi, step = meta[1:]
                lo_val, hi_val = st.slider(label, lo, hi, (lo, hi), step=step, key=f"sl_{key}")
                kind = "int" if isinstance(lo, int) else "float"
                search_space[key] = (kind, lo_val, hi_val, step)
            else:
                opts = meta[1]
                sel  = st.multiselect(f"{label} – Kandidaten", opts, opts, key=f"ms_{key}")
                search_space[key] = ("categorical", sel)

    st.info(f"🎯 Aktueller Suchraum:  {search_space}")

    # Defaults für fixe Parameter, falls nicht optimiert
    start_date_str = start_date.strftime("%Y-%m-%d")
    base_kwargs = {
        "start_balance":          100_000,
        "start_month":            start_date_str,
        "universe_mode":          "static",
        "rebalance_frequency":    "monthly",
        "custom_rebalance_months": 1,
        "enable_trading_costs":   False,
        "use_risk_overlay": False, 
    }
    if "num_stocks"  not in search_space:
        base_kwargs["num_stocks"] = st.number_input("Anzahl Aktien (fix)", 5, 50, 20, key="fix_num")
    if "window_days" not in search_space:
        base_kwargs["window_days"] = st.slider("Lookback Tage (fix)", 50, 500, 200, 10, key="fix_win")

    with st.expander("🎯 Objective-Gewichte"):
        kpi_weights = {
            "Sharpe Ratio": st.slider("Sharpe", 0.0, 3.0, 1.0, 0.1),
            "Ulcer Index":  -st.slider("Ulcer Index", 0.0, 3.0, 1.0, 0.1),
            "CAGR (%)":      st.slider("CAGR", 0.0, 3.0, 1.0, 0.1),
        }
    n_trials = st.number_input("Trials", 10, 500, 100, 10)

    if st.button("🚀 Suche starten"):
        study = run_optimizer(price_df, base_kwargs, search_space, kpi_weights, n_trials)
        show_study_results(study, kpi_weights, price_df, base_kwargs)

def show_study_results(study, kpi_weights, price_df, fixed_kwargs):
    
    # ------- A) Trials-DataFrame aufbereiten -----------------------
    df = study.trials_dataframe()

    # Optuna ≥ 4 → MultiIndex flatten
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            sec if main in ("params", "user_attrs") else main
            for main, sec in df.columns.to_list()
        ]

    # Optuna ≤ 3 → user_attrs aufsplitten
    if "user_attrs" in df.columns:
        df = pd.concat(
            [df.drop(columns=["user_attrs"]), df["user_attrs"].apply(pd.Series)],
            axis=1
        )

    # Präfixe entfernen
    df = df.rename(columns=lambda c: re.sub(r"^(param_|params_|user_attrs?_)", "", c))

    # KPI-Spalten ermitteln
    kpi_map = {"Sharpe Ratio": "Sharpe", "CAGR (%)": "CAGR", "Ulcer Index": "Ulcer Index"}
    kpis    = [kpi_map[k] for k in kpi_weights if kpi_map[k] in df.columns]

    # ------- B) Top 50 Runs ----------------------------------------
    cols_top = ["number", "value"] + kpis + [
        c for c in sorted(df.columns) if c not in ("number", "value", *kpis)
    ]
    top_df = df[cols_top].sort_values("value", ascending=False).head(50)

    st.subheader("🏆 Top 50 Runs")
    st.dataframe(top_df.style.hide(axis="index"), use_container_width=True)

    # Auswahl eines Runs für Backtest
    run_numbers = top_df["number"].tolist()
    selected = st.selectbox("Wähle Run-Nummer zum Backtesten", run_numbers)
    run_btn = st.button("🔄 Ausgewählten Run backtesten", key="run_selected_btn")
    if run_btn:
        # Parameter ins Session-State schreiben
        sel_row = df[df["number"] == selected].iloc[0]
        params = {c: sel_row[c] for c in sel_row.index if c not in ("number","value",*kpis)}
        for k, v in params.items():
            st.session_state[f"opt_{k}"] = v
        # Merke den gewählten Run
        st.session_state.selected_run = selected
        st.success(f"Run {selected} für Backtest ausgewählt und Parameter gespeichert.")

    # Wenn ein Run gewählt wurde, durchführen
    if st.session_state.get('selected_run', None) is not None:
        sel_num = st.session_state.selected_run
        sel_row = df[df["number"] == sel_num].iloc[0]
        params = {c: sel_row[c] for c in sel_row.index if c not in ("number","value",*kpis)}
        run_kwargs = {**fixed_kwargs, **params}
        if "num_stocks" not in run_kwargs:
            run_kwargs["num_stocks"] = fixed_kwargs.get("num_stocks")
        if "window_days" not in run_kwargs:
            run_kwargs["window_days"] = fixed_kwargs.get("window_days")

        eng_sel = SharpeBacktestEngine(price_df, **run_kwargs)
        eng_sel.run_with_next_month_allocation()

        st.subheader(f"🔍 Backtest-Ergebnisse für Run {sel_num}")
        st.markdown("---")
        st.subheader("🔍 KPI-Übersicht des gewählten Runs")
        if not eng_sel.performance_metrics.empty:
            st.dataframe(eng_sel.performance_metrics, hide_index=True, use_container_width=True)
        st.markdown("---")
        st.subheader("📈 Portfolio-Verlauf des gewählten Runs")
        if not eng_sel.portfolio_value.empty:
            st.line_chart(eng_sel.portfolio_value)

    # ------- C) Best-Run erneut ausführen --------------------------
    best_params = study.best_params
    run_kwargs  = {**fixed_kwargs, **best_params}
    if "num_stocks" not in run_kwargs:
        run_kwargs["num_stocks"] = fixed_kwargs.get("num_stocks")
    if "window_days" not in run_kwargs:
        run_kwargs["window_days"] = fixed_kwargs.get("window_days")

    eng_best = SharpeBacktestEngine(price_df, **run_kwargs)
    eng_best.run_with_next_month_allocation()

    st.markdown("---")
    st.write("Engine-Start:", eng_best.user_start_date)
    st.header("🚀 Details des Best-Runs")
    render_engine_tabs(eng_best)

    # ------- D) Best-Run KPIs & Parameter -------------------------
    best = top_df.iloc[0]
    param_cols = [c for c in best.index if c not in ("number", "value", *kpis)]

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🥇 Best-Run KPIs")
        st.table(
            best[kpis]
              .rename_axis("KPI")
              .to_frame("Wert")
        )
    with col2:
        st.subheader("⚙️ Best-Run Parameter")
        st.table(
            best[param_cols]
              .dropna()
              .rename_axis("Parameter")
              .to_frame("Wert")
        )

    # ------- E) Performance & Balance pro Jahr --------------------
    yearly_bal     = eng_best.portfolio_value.resample("YE").last()
    yearly_ret_pct = yearly_bal.pct_change().mul(100).round(1)

    df_year = pd.DataFrame({
        "Year":        yearly_bal.index.year,
        "Return (%)":  yearly_ret_pct,
        "Balance":     yearly_bal.round(0).astype(int),
    })
    df_year["Return (%)"] = df_year["Return (%)"].fillna("")

    st.subheader("📈 Performance & Balance pro Jahr des Best-Runs")
    st.table(df_year.astype({"Year": int}).reset_index(drop=True))

def render_engine_tabs(engine):
    tabs = st.tabs(["Dashboard", "Daily", "Monthly", "Yearly", "Drawdown"])

    # --- Dashboard ------------------------------------------------
    with tabs[0]:
        st.subheader("🔍 KPI-Übersicht")
        st.dataframe(engine.performance_metrics, hide_index=True, use_container_width=True)
        st.line_chart(engine.portfolio_value, height=250)

    # --- Daily ----------------------------------------------------
    with tabs[1]:
        st.subheader("📅 Daily Portfolio")
        st.dataframe(engine.daily_df, use_container_width=True)

    # --- Monthly --------------------------------------------------
    with tabs[2]:
        st.subheader("🗓️ Monthly Performance")
        st.dataframe(engine.monthly_performance, use_container_width=True)

    # --- Yearly ---------------------------------------------------
    with tabs[3]:
        yearly_bal = engine.portfolio_value.resample("YE").last()
        yearly_ret = yearly_bal.pct_change()*100
        df_year = pd.DataFrame({"Year": yearly_bal.index.year,
                                "Return (%)": yearly_ret,
                                "Balance": yearly_bal})
        st.dataframe(df_year.reset_index(drop=True), use_container_width=True)

    # --- Drawdown -------------------------------------------------
    with tabs[4]:
        df_port = engine.portfolio_value.to_frame("Portfolio")
        df_port["Peak"] = df_port["Portfolio"].cummax()
        df_port["Drawdown"] = df_port["Portfolio"] / df_port["Peak"] - 1
        dd = (df_port["Drawdown"]*100).round(2)
        st.line_chart(dd, height=250)


# =============================================================================
# === DATENLADE- UND KONVERTIERUNGSFUNKTIONEN (JETZT MAXIMAL ANGEPASST) ===
# =============================================================================

def convert_single_ticker_pricedata_to_ohlcv_df(
    price_data_list: List[Dict[str, Any]], 
    ticker: str,
    expected_start_date: pd.Timestamp,
    expected_end_date: pd.Timestamp
) -> pd.DataFrame:
    """
    Konvertiert die PriceData-Dict-Liste EINES Tickers in einen OHLCV DataFrame.
    Füllt fehlende Geschäftstage im Zeitraum aus.
    """
    empty_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    empty_df_with_index = pd.DataFrame(columns=empty_cols)
    empty_df_with_index.index.name = 'Date'

    if not price_data_list:
        return empty_df_with_index

    records = []
    min_date_in_data: Optional[pd.Timestamp] = None
    for record_dict in price_data_list:
        trade_dt_val = record_dict.get('trade_date')
        if trade_dt_val is None: 
            # st.warning(f"Datensatz für {ticker} ohne 'trade_date' übersprungen: {record_dict}")
            continue
        try:
            # Sicherstellen, dass trade_date ein Datumsobjekt oder ein String ist, der konvertiert werden kann
            if isinstance(trade_dt_val, dt.date):
                trade_dt = pd.to_datetime(trade_dt_val).tz_localize(None).normalize()
            elif isinstance(trade_dt_val, str):
                trade_dt = pd.to_datetime(trade_dt_val).tz_localize(None).normalize()
            else: # Fallback für andere Typen, z.B. Timestamp
                trade_dt = pd.to_datetime(str(trade_dt_val)).tz_localize(None).normalize()

            current_record = {'Date': trade_dt}
            for col_key_lower in ['open', 'high', 'low', 'close', 'volume']:
                col_key_capitalized = col_key_lower.capitalize()
                current_record[col_key_capitalized] = record_dict.get(col_key_lower)
            records.append(current_record)
            if min_date_in_data is None or trade_dt < min_date_in_data: 
                min_date_in_data = trade_dt
        except Exception as e_rec_proc: 
            st.warning(f"Fehler bei der Recordverarbeitung für {ticker} (Datum: {trade_dt_val}): {e_rec_proc}")
            
    if not records: return empty_df_with_index
    df = pd.DataFrame(records)
    if df.empty: return empty_df_with_index

    try: 
        df.set_index('Date', inplace=True)
        df.sort_index(inplace=True)
    except Exception as e_idx: 
        st.error(f"Fehler beim Setzen des Datumsindex für {ticker}: {e_idx}")
        return empty_df_with_index
    
    for col in empty_cols:
        if col in df.columns: 
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else: 
            df[col] = pd.NA 
    
    if not df.empty and min_date_in_data is not None:
        # Stelle sicher, dass expected_start/end_date Timestamps sind
        idx_start = pd.Timestamp(expected_start_date).normalize()
        idx_end = pd.Timestamp(expected_end_date).normalize()
        
        effective_idx_start = max(idx_start, df.index.min())
        full_idx = pd.date_range(start=effective_idx_start, end=idx_end, freq='B') # Business Day Frequenz
        df = df.reindex(full_idx)
        df.ffill(inplace=True)
        df.bfill(inplace=True)
    elif df.empty: 
        return empty_df_with_index
        
    return df


def prepare_eod_data_map_for_overlay(
    sdm_instance: StockDataManager, 
    indicator_configs: List[Dict[str, Any]], 
    history_start_date_str: str,
    current_processing_date_str: str
) -> Dict[str, pd.DataFrame]:
    eod_data_map: Dict[str, pd.DataFrame] = {}
    all_required_tickers = set(conf.get("asset","").upper() for conf in indicator_configs if conf.get("asset"))
    if not all_required_tickers: st.warning("Keine Assets in Overlay-Config."); return eod_data_map
    
    try:
        query_start_date = pd.to_datetime(history_start_date_str).date()
        query_end_date = pd.to_datetime(current_processing_date_str).date()
    except Exception as e: st.error(f"Fehler Datumsformat für DB-Abfrage: {e}"); return eod_data_map

    for ticker_symbol_config in all_required_tickers:
        ticker_upper = ticker_symbol_config.upper()
        try:
            price_data_dicts_for_ticker: List[Dict[str,Any]] = sdm_instance.get_price_data(
                tickers=[ticker_upper], start_date=query_start_date, end_date=query_end_date
            )
            if not price_data_dicts_for_ticker:
                st.warning(f"Keine Rohdaten für {ticker_upper} vom SDM für Zeitraum {query_start_date} bis {query_end_date}.")
                df_ohlcv = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']); df_ohlcv.index.name = 'Date'
            else:
                df_ohlcv = convert_single_ticker_pricedata_to_ohlcv_df(
                    price_data_dicts_for_ticker, ticker_upper,
                    pd.to_datetime(history_start_date_str).normalize(),
                    pd.to_datetime(current_processing_date_str).normalize()
                )
            eod_data_map[ticker_upper] = df_ohlcv
            if df_ohlcv.empty: st.warning(f"Leerer DF für '{ticker_upper}' nach Aufbereitung.")
        except Exception as e_ticker_load:
            st.error(f"Fehler Laden für {ticker_upper}: {e_ticker_load}"); st.exception(e_ticker_load)
            df_empty = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']); df_empty.index.name = 'Date'
            eod_data_map[ticker_upper] = df_empty
    return eod_data_map


# =============================================================================
# === RISK OVERLAY TEST UI (DEIN CODE, WIE ZULETZT BESPROCHEN) ===
# Hier wird die oben definierte prepare_eod_data_map_for_overlay verwendet
# =============================================================================
def show_risk_overlay_test_ui():
    # ... (Der Code für show_risk_overlay_test_ui bleibt genau so, wie ich ihn dir 
    #      im Post "kannst du mir die passage 1:1 geben damit ich sie reinkopieren kann?"
    #      gegeben habe. Er verwendet die Hilfsfunktionen oben.) ...
    # --- Stelle sicher, dass alle globalen Imports wie st, pd, json, etc. auch hier verfügbar sind ---
    st.title("🧪 Risk Overlay - Komponententest")

    if 'sdm_instance' not in st.session_state:
        try: st.session_state.sdm_instance = StockDataManager()
        except Exception as e: st.error(f"Fehler Initialisierung SDM: {e}"); st.exception(e); st.stop()
    sdm_instance_overlay = st.session_state.sdm_instance

    st.header("RiskOverlay Initialisierung")
    risk_overlay_instance: Optional[RiskOverlay] = None
    overlay_config_path_str_overlay = str(Path(CFG.RISK_OVERLAY["config_path"]))
    try:
        risk_overlay_instance = get_overlay() # Deine Caching-Funktion
        if not risk_overlay_instance: st.error("get_overlay() gab None zurück."); st.stop()
        st.success("RiskOverlay geladen!"); st.write(f"Overlay Config: `{overlay_config_path_str_overlay}`"); st.write(f"Anzahl Indikatoren: {len(risk_overlay_instance.indicators) if hasattr(risk_overlay_instance, 'indicators') else 'N/A'}")
    except Exception as e_ro_init_overlay: st.error(f"Fehler Laden RiskOverlay: {e_ro_init_overlay}"); st.exception(e_ro_init_overlay); st.stop()

    st.header("Daten für Overlay Test")
    indicator_configs_overlay = risk_overlay_instance.raw_indicators_config if risk_overlay_instance and hasattr(risk_overlay_instance, 'raw_indicators_config') else []
    if not indicator_configs_overlay: st.error("Keine Indikator-Configs in RiskOverlay."); st.stop()

    st.subheader("Testdatum für Overlay-Auswertung")
    default_proc_date_overlay = dt.date.today() - pd.Timedelta(days=1) 
    proc_date_input_overlay = st.date_input("Datum", value=default_proc_date_overlay, max_value=dt.date.today(), key="overlay_proc_date_main_v2")
    lookback_years_overlay = st.slider("Historie (Jahre)", 1, 15, 5, key="hist_years_slider_main_overlay_v2")
    hist_start_dt_overlay = (pd.to_datetime(proc_date_input_overlay) - pd.DateOffset(years=lookback_years_overlay)).replace(day=1).date()
    hist_start_str_overlay = hist_start_dt_overlay.strftime('%Y-%m-%d')
    proc_date_str_overlay = proc_date_input_overlay.strftime('%Y-%m-%d')

    if st.button("1. EOD-Daten für Overlay laden", key="load_eod_overlay_btn_main_v3"):
        with st.spinner("Lade und bereite EOD-Daten vor..."):
            eod_map_overlay = prepare_eod_data_map_for_overlay(
                sdm_instance_overlay, indicator_configs_overlay,
                hist_start_str_overlay, proc_date_str_overlay
            )
        st.session_state.eod_data_map_for_overlay_test_main = eod_map_overlay
        if not eod_map_overlay or any(df.empty for df in eod_map_overlay.values()): st.error("Einige/alle Daten fehlen.")
        else: st.success("EOD-Daten für Overlay-Test vorbereitet.")

    current_assets_in_cfg_overlay = set(conf.get("asset","").upper() for conf in indicator_configs_overlay if conf.get("asset"))
    if 'eod_data_map_for_overlay_test_main' in st.session_state and st.session_state.eod_data_map_for_overlay_test_main:
        st.header("RiskOverlay Ausführung")
        current_eod_data_map_overlay = st.session_state.eod_data_map_for_overlay_test_main
        current_proc_date_ts_overlay = pd.to_datetime(proc_date_input_overlay)

        if st.button("2. Risk Overlay ausführen", key="run_overlay_btn_main_v4"):
            try:
                with st.spinner("RiskOverlay läuft..."):
                    if hasattr(risk_overlay_instance, 'score_log'): risk_overlay_instance.score_log = [] 
                    final_quote_overlay = risk_overlay_instance.run_daily_overlay(current_date=current_proc_date_ts_overlay, eod_data_map=current_eod_data_map_overlay)
                st.metric("Finale Aktienquote", f"{final_quote_overlay:.2%}")
                st.subheader("Overlay Log:")
                if risk_overlay_instance.score_log:
                    def serialize_log_entry(entry): new_entry = {};_ = [new_entry.update({k: v.isoformat()}) if isinstance(v, (pd.Timestamp, date, dt.date)) else new_entry.update({k:(str(v) if isinstance(v, float) and (pd.isna(v) or np.isinf(v) or np.isnan(v)) else v)}) for k,v in entry.items()]; return new_entry
                    st.json(serialize_log_entry(risk_overlay_instance.score_log[-1]))
                else: st.write("Kein Log-Eintrag.")
            except Exception as e_run_overlay: st.error(f"Fehler run_daily_overlay: {e_run_overlay}"); st.exception(e_run_overlay)
    elif current_assets_in_cfg_overlay:
        st.info("Bitte EOD-Daten laden (Button '1. ...').")
    st.markdown("---"); st.info("Ende Risk Overlay Test.")

# -----------------------------------------------------------------------------
# 5) Router
# -----------------------------------------------------------------------------
if page == "Backtester":
    show_backtester_ui()
elif page == "Optimizer":
    show_optimizer_ui()
elif page =="Risk Overlay Test":
    show_risk_overlay_test_ui()
else:
    show_data_ui()

