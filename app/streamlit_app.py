import streamlit as st
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
import datetime as dt
from pandas.tseries.offsets import BDay
import tempfile
import os
from sqlmodel import select
import re
import plotly.graph_objects as go
from AlphaMachine_core.models import TickerPeriod
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

init_db() 

# -----------------------------------------------------------------------------
# 1) Page-Config
# -----------------------------------------------------------------------------
st.set_page_config("AlphaMachine", layout="wide")

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
page = st.sidebar.radio("🗂️ Seite wählen",["Backtester", "Optimizer", "Data Mgmt"],index=0)

# -----------------------------------------------------------------------------
# 4) CSV-Loader (Session-Cache)
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner="📂 CSV wird geladen…")
def load_csv(file):
    return pd.read_csv(file, index_col=0, parse_dates=True)


# -----------------------------------------------------------------------------
# Load Prices
# -----------------------------------------------------------------------------
def load_price_df(dm, tickers, start_date, end_date, window_days, lookback_margin_days=20):
    # Berechne das Lookback in HANDELSTAGEN!
    price_start = pd.to_datetime(start_date) - BDay(window_days + lookback_margin_days)
    price_start = price_start.date()  # für get_price_data als String
    raw = dm.get_price_data(
        tickers,
        price_start.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )
    if not raw:
        return pd.DataFrame(), price_start

    price_df = (
        pd.DataFrame([r.model_dump() for r in raw])
          .assign(date=lambda d: pd.to_datetime(d["trade_date"]))
          .pivot(index="date", columns="ticker", values="close")
          .sort_index()
    )
    full_idx = pd.date_range(price_start, end_date, freq=BDay())
    price_df = price_df.reindex(full_idx).ffill()
    return price_df, price_start




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

    
    # ### OPTIMIZER START – Sidebar‑Widgets  ###
    st.sidebar.markdown("---")
    st.sidebar.header("🚀 Optimizer")

    _kpi_weights = {
        "Sharpe Ratio": st.sidebar.slider("Sharpe‑Gewicht", 0.0, 3.0, 1.0, 0.1),
        "Ulcer Index":  -st.sidebar.slider("Ulcer‑Gewicht",  0.0, 3.0, 1.0, 0.1),
        "CAGR (%)":     st.sidebar.slider("CAGR‑Gewicht",   0.0, 3.0, 1.0, 0.1),
    }

    _opt_trials  = st.sidebar.number_input("Versuche", 10, 500, 50, 10)
    
    # --- Buttons -----------------------------------------------
    run_opt_btn = st.sidebar.button("Optimizer starten 🚀")
    run_btn     = st.sidebar.button("Backtest starten 🚀")

    # Wenn *keiner* gedrückt wurde → zurück
    if not run_btn and not run_opt_btn:
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
        # 1) Baseline (ohne Overlay)
        engine_baseline = SharpeBacktestEngine(
            price_df,
            start_balance,
            num_stocks,
            #start_month=month,
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
        )
        engine_baseline.risk_overlay = None  # <--- Overlay AUS!
        engine_baseline.run_with_next_month_allocation()

        # 2) Overlay (mit Risk On/Off)
        engine_overlay = SharpeBacktestEngine(
            price_df,
            start_balance,
            num_stocks,
            #start_month=month,
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
        )
        engine_overlay.run_with_next_month_allocation()

        # collect infos for Parameter tab
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
            "Custom Monate":        custom_months if rebalance_freq=="custom" else "-",
            "Min Weight (%)":       round(min_w*100,2),
            "Max Weight (%)":       round(max_w*100,2),
            "Force Equal Weight":   force_eq,
            "Trading-Kosten aktiv": enable_tc,
            "Fixe Kosten/Trade":    fixed_cost,
            "Variable Kosten (%)":  round(var_cost*100,2)
        }

        #DEBUG
        #st.write(f"🔎 running backtest on {price_df.shape[0]} days × {price_df.shape[1]} tickers")
        #----------------------


        #DEBUG
        #st.write("🔎 final portfolio_value:", engine.portfolio_value.tail())
        #st.write("🔎 performance_metrics:", engine.performance_metrics)
        #------------------------

    msg = "Backtest fertig ✅"
    if available < orig_num_stocks:
        msg += f"  (Achtung: nur {available} Stocks vorhanden statt {orig_num_stocks})"
    st.success(msg)

    # Tabs
    tabs = st.tabs([
        "Dashboard",
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

    with tabs[0]:
        st.subheader("🔍 KPI-Vergleich: Baseline vs. Risk-Overlay")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Baseline**")
            st.dataframe(engine_baseline.performance_metrics, hide_index=True, use_container_width=True)
        with col2:
            st.markdown("**Risk-On/Risk-Off**")
            st.dataframe(engine_overlay.performance_metrics, hide_index=True, use_container_width=True)

        st.markdown("---")
        st.subheader("📈 Portfolio-Verlauf")
        st.line_chart(
            pd.DataFrame({
                "Baseline": engine_baseline.portfolio_value,
                "Risk-On/Risk-Off": engine_overlay.portfolio_value,
            })
        )

        st.markdown("---")
        st.subheader("📆 Monatliche Performance (%)")
        perf_df = pd.DataFrame({
            "Baseline": engine_baseline.monthly_performance.set_index("Date")["Monthly PnL (%)"],
            "Risk-On/Risk-Off": engine_overlay.monthly_performance.set_index("Date")["Monthly PnL (%)"],
        })
        st.bar_chart(perf_df)

    with tabs[1]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📅 Daily Portfolio Baseline")
            if not engine_baseline.daily_df.empty:
                st.dataframe(engine_baseline.daily_df, use_container_width=True)
            else:
                st.info("Keine Daily-Daten für Baseline.")
        with col2:
            st.subheader("📅 Daily Portfolio Overlay")
            if not engine_overlay.daily_df.empty:
                st.dataframe(engine_overlay.daily_df, use_container_width=True)
            else:
                st.info("Keine Daily-Daten für Overlay.")


    with tabs[2]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🗓️ Monthly Performance Baseline")
            if not engine_baseline.monthly_performance.empty:
                st.dataframe(engine_baseline.monthly_performance, use_container_width=True)
            else:
                st.info("Keine Monatsdaten für Baseline.")
        with col2:
            st.subheader("🗓️ Monthly Performance Overlay")
            if not engine_overlay.monthly_performance.empty:
                st.dataframe(engine_overlay.monthly_performance, use_container_width=True)
            else:
                st.info("Keine Monatsdaten für Overlay.")

    with tabs[3]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🗓️ Yearly Performance Baseline")
            if not engine_baseline.portfolio_value.empty:
                yearly_balance = engine_baseline.portfolio_value.resample("YE").last()
                yearly_start = engine_baseline.portfolio_value.resample("YE").first()
                yearly_pnl = yearly_balance - yearly_start
                yearly_ret = yearly_balance.pct_change() * 100

                # Summiere die Monthly PnL für jedes Jahr
                monthly_pnl = engine_baseline.monthly_performance.copy()
                monthly_pnl["Year"] = pd.to_datetime(monthly_pnl["Date"]).dt.year
                yearly_monthly_pnl = monthly_pnl.groupby("Year")["Monthly PnL ($)"].sum()

                df = pd.DataFrame({
                    "Year": yearly_balance.index.year,
                    "Yearly PnL (Portfolio Value)": yearly_pnl.values,
                    "Yearly PnL (Sum Monthly)": yearly_monthly_pnl.reindex(yearly_balance.index.year).values,
                    "Return (%)": yearly_ret.values,
                    "Balance": yearly_balance.values
                }).reset_index(drop=True)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("Keine Jahresdaten für Baseline.")
        with col2:
            st.subheader("🗓️ Yearly Performance Overlay")
            if not engine_overlay.portfolio_value.empty:
                yearly_balance = engine_overlay.portfolio_value.resample("YE").last()
                yearly_ret = yearly_balance.pct_change() * 100
                df = pd.DataFrame({
                    "Year": yearly_balance.index.year,
                    "Return (%)": yearly_ret,
                    "Balance": yearly_balance
                }).reset_index(drop=True)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("Keine Jahresdaten für Overlay.")



    with tabs[4]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📊 Monthly Allocation Baseline")
            if not engine_baseline.monthly_allocations.empty:
                df_sorted = engine_baseline.monthly_allocations.sort_values(
                    by="Rebalance Date", ascending=False
                )
                st.dataframe(df_sorted, use_container_width=True)
            else:
                st.info("Keine Daten für Baseline.")
        with col2:
            st.subheader("📊 Monthly Allocation Overlay")
            if not engine_overlay.monthly_allocations.empty:
                df_sorted = engine_overlay.monthly_allocations.sort_values(
                    by="Rebalance Date", ascending=False
                )
                st.dataframe(df_sorted, use_container_width=True)
            else:
                st.info("Keine Daten für Overlay.")


    with tabs[5]:
        st.subheader("🔮 Next Month Allocation Baseline")
        if hasattr(engine_baseline, "next_month_weights"):
            df_next = (
                engine_baseline.next_month_weights
                    .mul(100)
                    .reset_index()
            )
            df_next.columns = ["Ticker", "Gewicht (%)"]
            st.dataframe(df_next, use_container_width=True)
        else:
            st.info("Keine Auswahl für den Folgemonat (Baseline).")

        st.subheader("🔮 Next Month Allocation Overlay")
        if hasattr(engine_overlay, "next_month_weights"):
            df_next = (
                engine_overlay.next_month_weights
                    .mul(100)
                    .reset_index()
            )
            df_next.columns = ["Ticker", "Gewicht (%)"]
            st.dataframe(df_next, use_container_width=True)
        else:
            st.info("Keine Auswahl für den Folgemonat (Overlay).")


    with tabs[6]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📉 Top 10 Drawdowns Baseline")
            df_port = engine_baseline.portfolio_value.to_frame(name="Portfolio")
            df_port["Peak"] = df_port["Portfolio"].cummax()
            df_port["Drawdown"] = df_port["Portfolio"] / df_port["Peak"] - 1
            periods = []
            in_dd = False
            for date, row in df_port.iterrows():
                if not in_dd and row["Drawdown"] < 0:
                    in_dd = True
                    start = date
                    peak_val = row["Peak"]
                    trough_val = row["Portfolio"]
                    trough = date
                elif in_dd:
                    if row["Portfolio"] < trough_val:
                        trough_val = row["Portfolio"]
                        trough = date
                    if row["Portfolio"] >= peak_val:
                        periods.append({
                            "Start":            start.date(),
                            "Trough":           trough.date(),
                            "End":              date.date(),
                            "Length (Days)":    (date - start).days,
                            "Recovery Time":    (date - trough).days,
                            "Drawdown (%)":     round((trough_val/peak_val - 1)*100, 2),
                        })
                        in_dd = False
            if in_dd:
                last_date = df_port.index[-1]
                periods.append({
                    "Start":         start.date(),
                    "Trough":        trough.date(),
                    "End":           last_date.date(),
                    "Length (Days)": (last_date - start).days,
                    "Recovery Time": None,
                    "Drawdown (%)":  round((trough_val/peak_val - 1)*100, 2),
                })
            df_dd = (
                pd.DataFrame(periods)
                .sort_values(by="Drawdown (%)")
                .head(10)
                .reset_index(drop=True)
            )
            if not df_dd.empty:
                st.dataframe(df_dd, use_container_width=True)
            else:
                st.info("Keine Drawdown-Daten für Baseline.")
        with col2:
            st.subheader("📉 Top 10 Drawdowns Overlay")
            df_port = engine_overlay.portfolio_value.to_frame(name="Portfolio")
            df_port["Peak"] = df_port["Portfolio"].cummax()
            df_port["Drawdown"] = df_port["Portfolio"] / df_port["Peak"] - 1
            periods = []
            in_dd = False
            for date, row in df_port.iterrows():
                if not in_dd and row["Drawdown"] < 0:
                    in_dd = True
                    start = date
                    peak_val = row["Peak"]
                    trough_val = row["Portfolio"]
                    trough = date
                elif in_dd:
                    if row["Portfolio"] < trough_val:
                        trough_val = row["Portfolio"]
                        trough = date
                    if row["Portfolio"] >= peak_val:
                        periods.append({
                            "Start":            start.date(),
                            "Trough":           trough.date(),
                            "End":              date.date(),
                            "Length (Days)":    (date - start).days,
                            "Recovery Time":    (date - trough).days,
                            "Drawdown (%)":     round((trough_val/peak_val - 1)*100, 2),
                        })
                        in_dd = False
            if in_dd:
                last_date = df_port.index[-1]
                periods.append({
                    "Start":         start.date(),
                    "Trough":        trough.date(),
                    "End":           last_date.date(),
                    "Length (Days)": (last_date - start).days,
                    "Recovery Time": None,
                    "Drawdown (%)":  round((trough_val/peak_val - 1)*100, 2),
                })
            df_dd = (
                pd.DataFrame(periods)
                .sort_values(by="Drawdown (%)")
                .head(10)
                .reset_index(drop=True)
            )
            if not df_dd.empty:
                st.dataframe(df_dd, use_container_width=True)
            else:
                st.info("Keine Drawdown-Daten für Overlay.")


    # Tab 5: Trading Costs
    with tabs[7]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("💸 Trading Costs Baseline")
            if not engine_baseline.monthly_allocations.empty and "Trading Costs" in engine_baseline.monthly_allocations:
                cost_df = (
                    engine_baseline.monthly_allocations
                        .dropna(subset=["Trading Costs"])
                        .groupby("Rebalance Date")["Trading Costs"]
                        .sum()
                        .reset_index(name="Total Trading Costs")
                )
                st.dataframe(cost_df, use_container_width=True)
            else:
                st.info("Keine Trading-Kosten-Daten für Baseline.")
        with col2:
            st.subheader("💸 Trading Costs Overlay")
            if not engine_overlay.monthly_allocations.empty and "Trading Costs" in engine_overlay.monthly_allocations:
                cost_df = (
                    engine_overlay.monthly_allocations
                        .dropna(subset=["Trading Costs"])
                        .groupby("Rebalance Date")["Trading Costs"]
                        .sum()
                        .reset_index(name="Total Trading Costs")
                )
                st.dataframe(cost_df, use_container_width=True)
            else:
                st.info("Keine Trading-Kosten-Daten für Overlay.")


    # Tab 6: Rebalance Analysis
    with tabs[8]:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("🔁 Rebalance Analysis Baseline")
            df_reb = pd.DataFrame(engine_baseline.selection_details)
            df_reb = df_reb[df_reb["Rebalance Date"] != "SUMMARY"].copy()
            if len(df_reb) > 1:
                df_reb["Rebalance Date"] = pd.to_datetime(df_reb["Rebalance Date"])
                df_reb["Days Since Last"] = df_reb["Rebalance Date"].diff().dt.days
            st.dataframe(df_reb, use_container_width=True)
        with col2:
            st.subheader("🔁 Rebalance Analysis Overlay")
            df_reb = pd.DataFrame(engine_overlay.selection_details)
            df_reb = df_reb[df_reb["Rebalance Date"] != "SUMMARY"].copy()
            if len(df_reb) > 1:
                df_reb["Rebalance Date"] = pd.to_datetime(df_reb["Rebalance Date"])
                df_reb["Days Since Last"] = df_reb["Rebalance Date"].diff().dt.days
            st.dataframe(df_reb, use_container_width=True)


    # Tab 7: Parameters
    with tabs[9]:
        st.subheader("⚙️ Ausgewählte Backtest-Parameter")
        df_params = pd.DataFrame(ui_params.items(), columns=["Parameter", "Wert"])
        df_params["Wert"] = df_params["Wert"].astype(str)
        st.dataframe(df_params, use_container_width=True)

    # Tab 8: Logs
    with tabs[10]:
        st.subheader("🪵 Logs")
        for line in engine_baseline.ticker_coverage_logs + engine_baseline.log_lines:
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

        # Overlay-Report
        path_overlay = os.path.join(tmp_dir, f"AlphaMachine_Overlay_{dt.date.today()}.xlsx")
        export_results_to_excel(engine_overlay, path_overlay)
        with open(path_overlay, "rb") as f:
            st.download_button(
                "📥 Excel-Report Overlay",
                f.read(),
                file_name=os.path.basename(path_overlay),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

  

# =============================================================================
# === Data-Management-UI ===
# =============================================================================
def show_data_ui():
    st.header("📂 Data Management")
    dm = StockDataManager()

    mode = st.radio("Modus", ["➕ Add/Update", "👁️ View/Delete"], index=0)

    if mode == "➕ Add/Update":
        st.subheader("➕ Ticker einfügen & Daten updaten")
        tickers = st.text_area("Tickers (eine pro Zeile)", height=120)
        month_dt = st.date_input("Monat wählen", value=dt.date.today().replace(day=1))
        start = month_dt.replace(day=1)
        end = (pd.to_datetime(start) + pd.offsets.MonthEnd(1)).date()
        st.write(f"Zeitraum: {start} bis {end}")
        
        # erst bestehende Quellen aus der DB holen (plus Default-Werte)
        with get_session() as session:
            existing = list(session.exec(select(TickerPeriod.source)).unique())
        defaults = ["Topweights"]
        options = sorted(set(existing + defaults))
        options.append("Andere…")
        source = st.selectbox("Quelle", options)
        # wenn „Andere…“ ausgewählt, zeige ein Textfeld
        if source == "Andere…":
            custom = st.text_input("Neue Quelle eingeben")
            # sobald der User etwas eintippt, verwenden wir das
            if custom:
                source = custom
        
        if st.button("➕ Hinzufügen"):
            ts = [t.strip() for t in tickers.splitlines() if t.strip()]
            added = dm.add_tickers_for_period(ts, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), source)
            st.success(f"{len(added)} Ticker hinzugefügt.")

        if st.button("🔄 Preise updaten"):
            with get_session() as session:
                tickers_db = session.exec(select(TickerPeriod.ticker)).unique()
                tickers_db = [t for t in tickers_db]
            if not tickers_db:
                st.info("Keine Ticker in der DB zum Updaten.")
                return
            progress = st.progress(0.0)
            status = st.empty()
            updated = []
            for idx, tk in enumerate(tickers_db):
                status.info(f"📡 Lade Preise für {tk} …")
                success = dm.update_ticker_data([tk])
                updated += success
                progress.progress((idx + 1) / len(tickers_db))
            status.success("✅ Alle Ticker geladen.")
            st.success(f"{len(updated)} von {len(tickers_db)} Ticker aktualisiert.")
        return  # hier bleiben wir im Add/Update-Modus und brechen ab

    # ——— View/Delete Mode —————————————————————————————————————————————————————————
    st.subheader("👁️ View/Delete")
    with get_session() as session:
        all_periods = session.exec(select(TickerPeriod)).all()
    Monate  = sorted({p.start_date.strftime("%Y-%m") for p in all_periods})
    Quellen = sorted({p.source for p in all_periods})
    month   = st.selectbox("Monat",  Monate)
    source  = st.selectbox("Quelle", Quellen)

    periods = dm.get_periods(month, source)
    if periods:
        dfp = pd.DataFrame([vars(p) for p in periods])[['id','ticker','start_date','end_date','source']]
        st.dataframe(dfp.set_index('id'), use_container_width=True)
        to_del = st.multiselect("Zu löschen (ID)", dfp['id'].tolist())
        if st.button("🗑️ Löschen"):
            for pid in to_del:
                dm.delete_period(pid)
            st.success(f"{len(to_del)} Einträge gelöscht.")
            st.experimental_rerun()
            return
    else:
        st.info("Keine Period-Einträge für diesen Monat/Quelle.")
        # kein return hier, wir wollen trotzdem TickerInfo sehen

    st.markdown("---")
    st.subheader("Ticker Info")
    info = dm.get_ticker_info()
    if not info:
        st.info("Keine TickerInfo vorhanden.")
        return   # hier abbrechen, weil kein dfi gebildet werden kann

    # DataFrame für TickerInfo bauen und optional filtern
    dfi = pd.DataFrame([vars(i) for i in info]).drop(columns=["_sa_instance_state"], errors="ignore")
    all_cols = list(dfi.columns)
    filter_col = st.selectbox("Filter-Spalte", ["(kein)"] + all_cols, index=0)
    if filter_col != "(kein)":
        choices = sorted(dfi[filter_col].dropna().unique())
        sel = st.multiselect(f"Werte in «{filter_col}»", choices, default=choices)
        dfi = dfi[dfi[filter_col].isin(sel)]
    st.dataframe(dfi.set_index("id"), use_container_width=True)

    # ——— Price Chart ——————————————————————————————————————————————————————————————————
    st.markdown("---")
    st.subheader("📈 Price Chart")
    
    ticker_sel = st.selectbox("Welchen Ticker charten?", sorted(dfi["ticker"].unique()))
    default_start = dfi.loc[dfi["ticker"] == ticker_sel, "actual_start_date"].min()
    default_end   = dfi.loc[dfi["ticker"] == ticker_sel, "actual_end_date"].max()
    start_sel, end_sel = st.date_input("Zeitraum wählen", value=(default_start, default_end))

    raw = dm.get_price_data([ticker_sel], start_sel.strftime("%Y-%m-%d"), end_sel.strftime("%Y-%m-%d"))

    #DEBUG
    #st.write(f"🔎 got {len(raw)} raw price records")
    #if raw:
    #    st.write(raw[:3])  # oder raw[0].model_dump() / raw[0].dict()
    
    records = [{"date":r.trade_date, "open":r.open, "high":r.high, "low":r.low, "close":r.close, "volume":r.volume} for r in raw]
    pdf = pd.DataFrame(records)
    if pdf.empty:
        st.info("Keine Preisdaten im gewählten Zeitraum.")
        return

    pdf["date"] = pd.to_datetime(pdf["date"])
    pdf = pdf.sort_values("date").set_index("date")
    fig = go.Figure(data=[go.Candlestick(
        x=pdf.index, open=pdf["open"], high=pdf["high"],
        low=pdf["low"], close=pdf["close"], name=ticker_sel
    )])
    fig.update_layout(title=f"Candlestick for {ticker_sel}", xaxis_title="Date", yaxis_title="Price")
    st.plotly_chart(fig, use_container_width=True)

    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    full_range = pd.date_range(start_sel, end_sel, freq=us_bd)
    missing = full_range.difference(pdf.index)
    if not missing.empty:
        st.warning(f"⚠️ {len(missing)} Handelstage ohne Daten:")
        st.write(missing.strftime("%Y-%m-%d").tolist())

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



# -----------------------------------------------------------------------------
# 5) Router
# -----------------------------------------------------------------------------
if page == "Backtester":
    show_backtester_ui()
elif page == "Optimizer":
    show_optimizer_ui()
else:
    show_data_ui()
