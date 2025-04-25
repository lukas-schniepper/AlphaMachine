import streamlit as st
import pandas as pd
import datetime as dt
import tempfile, os
from dateutil.relativedelta import relativedelta

from AlphaMachine_core.engine import SharpeBacktestEngine
from AlphaMachine_core.reporting_no_sparklines import export_results_to_excel
from AlphaMachine_core.data_manager import StockDataManager            # <-- neu
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

# -----------------------------------------------------------------------------
# Page-Config
# -----------------------------------------------------------------------------
st.set_page_config("AlphaMachine", layout="wide")

# -----------------------------------------------------------------------------
# Passwort-Gate
# -----------------------------------------------------------------------------
pwd = st.sidebar.text_input("Passwort", type="password")
if pwd != st.secrets["APP_PW"]:
    st.warning("🔒 Bitte korrektes Passwort eingeben.")
    st.stop()
    

# -----------------------------------------------------------------------------
# Navigation (Backtester | Data Mgmt)
# -----------------------------------------------------------------------------
page = st.sidebar.radio("🗂️ Seite wählen", ["Backtester", "Data Mgmt"], index=0)

# -----------------------------------------------------------------------------
# Hilfs-Cache
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner="📂 CSV wird geladen…")
def load_csv(file):
    return pd.read_csv(file, index_col=0, parse_dates=True)

# -----------------------------------------------------------------------------
# === Backtester-UI ===
# -----------------------------------------------------------------------------
def show_backtester_ui():
    st.sidebar.header("📊 Backtest-Parameter")

    uploaded          = st.sidebar.file_uploader("CSV-Preisdaten", type="csv")
    start_balance     = st.sidebar.number_input("Startkapital", 10_000, 1_000_000, 100_000, 1_000)
    num_stocks        = st.sidebar.slider("Aktien pro Portfolio", 5, 50, 20)

    opt_method    = st.sidebar.selectbox("Optimierer",
                        ["ledoit-wolf", "minvar", "hrp"],
                        index=["ledoit-wolf","minvar","hrp"].index(CFG_OPT_METHOD))
    cov_estimator = st.sidebar.selectbox("Kovarianzschätzer",
                        ["ledoit-wolf", "constant-corr", "factor-model"],
                        index=["ledoit-wolf","constant-corr","factor-model"].index(CFG_COV_EST))
    opt_mode      = st.sidebar.selectbox("Optimierungsmodus",
                        ["select-then-optimize", "optimize-subset"],
                        index=["select-then-optimize","optimize-subset"].index(CFG_OPT_MODE))
    rebalance_freq = st.sidebar.selectbox("Rebalance",
                        ["weekly", "monthly", "custom"],
                        index=["weekly","monthly","custom"].index(CFG_REBAL_FREQ))
    custom_months = (st.sidebar.slider("Monate zwischen Rebalances", 1, 12, CFG_CUSTOM_REBAL)
                     if rebalance_freq=="custom" else 1)

    window_days = st.sidebar.slider("Lookback Days", 50, 500, CFG_WINDOW, 10)

    min_w = st.sidebar.slider("Min Weight (%)", 0.0, 5.0, float(CFG_MIN_W*100), 0.5) / 100
    max_w = st.sidebar.slider("Max Weight (%)", 5.0, 50.0, float(CFG_MAX_W*100), 1.0) / 100
    force_eq = st.sidebar.checkbox("Force Equal Weight", CFG_FORCE_EQ)

    st.sidebar.subheader("Trading-Kosten")
    enable_tc  = st.sidebar.checkbox("Kosten aktiv", CFG_ENABLE_TC)
    fixed_cost = st.sidebar.number_input("Fixe Kosten pro Trade", 0.0, 100.0, float(CFG_FIXED_COST))
    var_cost   = st.sidebar.number_input("Variable Kosten (%)", 0.0, 1.0, float(CFG_VAR_COST*100)) / 100

    run_btn = st.sidebar.button("Backtest starten 🚀")

    # --- Early Exit ----------------------------------------------------
    if run_btn and uploaded is None:
        st.warning("Bitte zuerst eine CSV-Datei hochladen.")
        st.stop()

    # --- Haupt-Logik ---------------------------------------------------
    if run_btn and uploaded:
        with st.spinner(f"📈 Backtest für {uploaded.name} läuft…"):
            price_df = load_csv(uploaded)
            if price_df.empty:
                st.error("Die hochgeladene CSV enthält keine Daten!")
                st.stop()

            progress = st.progress(0, text="Starte Optimierer…")
            engine = SharpeBacktestEngine(
                price_df,
                start_balance,
                num_stocks,
                optimizer_method   = opt_method,
                cov_estimator      = cov_estimator,
                rebalance_frequency= rebalance_freq,
                custom_rebalance_months=custom_months,
                window_days        = window_days,
                min_weight         = min_w,
                max_weight         = max_w,
                force_equal_weight = force_eq,
                enable_trading_costs = enable_tc,
                fixed_cost_per_trade = fixed_cost,
                variable_cost_pct    = var_cost,
            )
            engine.optimization_mode = opt_mode

            progress.progress(50, text="Rebalancing & Performance…")
            engine.run_with_next_month_allocation()
            progress.progress(100, text="Fertig!")

        st.success("Backtest fertig ✅")

        # -------- Tabs (Portfolio zuerst) ------------------------------
        tabs = st.tabs([
            "Portfolio", "Dashboard", "Daily", "Monthly Allocation",
            "Performance", "Risk", "Drawdowns", "Trading Costs",
            "Rebalance", "Selection", "Logs"
        ])

        # Portfolio
        with tabs[0]:
            st.subheader("📈 Portfolio-Verlauf")
            if not engine.portfolio_value.empty:
                st.line_chart(engine.portfolio_value)

        # Dashboard / KPI
        with tabs[1]:
            if not engine.performance_metrics.empty:
                st.subheader("🔍 KPI-Übersicht")
                st.dataframe(engine.performance_metrics, hide_index=True, use_container_width=True)

        # Daily
        with tabs[2]:
            if not engine.daily_df.empty:
                st.subheader("📅 Daily Portfolio Details")
                st.dataframe(engine.daily_df, use_container_width=True)

        # Monthly Allocation
        with tabs[3]:
            if not engine.monthly_allocations.empty:
                st.subheader("📊 Monthly Allocation Summary")
                st.dataframe(engine.monthly_allocations, use_container_width=True)

        # Performance (Monthly PnL)
        with tabs[4]:
            if not engine.monthly_performance.empty:
                st.subheader("📆 Monatliche Performance (%)")
                st.bar_chart(engine.monthly_performance.set_index("Date")["Monthly PnL (%)"])

        # Risk
        with tabs[5]:
            try:
                risk_df = engine.performance_metrics    # oder Sheet.
                st.subheader("⚠️ Risiko-Kennzahlen")
                st.dataframe(risk_df, use_container_width=True)
            except Exception:
                st.info("Risk-Sheet nicht verfügbar.")

        # Drawdowns
        with tabs[6]:
            try:
                dd_df = pd.read_excel(tmp_path, sheet_name="Drawdowns")
                st.subheader("📉 Drawdowns")
                st.dataframe(dd_df, use_container_width=True)
            except Exception:
                st.info("Drawdown-Sheet nicht verfügbar.")

        # Trading Costs
        with tabs[7]:
            try:
                tc_df = pd.read_excel(tmp_path, sheet_name="Trading Costs Summary")
                st.subheader("💸 Trading-Kosten")
                st.dataframe(tc_df, use_container_width=True)
            except Exception:
                st.info("Trading-Kosten-Sheet nicht verfügbar.")

        # Rebalance
        with tabs[8]:
            try:
                reb_df = pd.read_excel(tmp_path, sheet_name="Rebalance Analysis")
                st.subheader("🔁 Rebalance-Analyse")
                st.dataframe(reb_df, use_container_width=True)
            except Exception:
                st.info("Rebalance-Sheet nicht verfügbar.")

        # Selection Details
        with tabs[9]:
            if engine.selection_details:
                st.subheader("🔍 Selection Details")
                st.dataframe(pd.DataFrame(engine.selection_details), use_container_width=True)

        # Logs
        with tabs[10]:
            st.subheader("🪵 Run-Logs")
            if engine.log_lines:
                st.text("\n".join(engine.log_lines))

        # Excel-Download
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, "AlphaMachine_Report.xlsx")
            export_results_to_excel(engine, tmp_path)
            with open(tmp_path, "rb") as f:
                st.download_button(
                    "📥 Excel-Report",
                    f.read(),
                    file_name=f"AlphaMachine_{dt.date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

    else:
        st.info("Lade eine CSV hoch, wähle Parameter und starte den Backtest.")

# -----------------------------------------------------------------------------
# === Data-Management-UI (Minimal) ===
# -----------------------------------------------------------------------------
def show_data_ui():
    st.header("📂 Data Management")

    dm = StockDataManager(base_folder=os.path.expanduser("~/data_alpha"))

    st.subheader("Ticker für Monat definieren")
    # 1) Textarea für die Ticker-Liste
    tickers_text = st.text_area(
        "Tickers (eine pro Zeile)",
        placeholder="z.B. AAPL\nMSFT\nGOOGL",
        height=120
    )

    # 2) Monat wählen (Date-Input, Tag wird ignoriert)
    month_dt = st.date_input(
        "Monat auswählen",
        value=dt.date.today().replace(day=1)
    )
    # automatisch erster Tag des Monats
    period_start = month_dt.replace(day=1)
    # letzter Tag des Monats
    period_end   = (pd.to_datetime(period_start) + pd.offsets.MonthEnd(1)).date()

    st.write(f"Zeitraum: **{period_start}** bis **{period_end}**")

    # 3) Quelle (Source) auswählen
    source = st.selectbox(
        "Quelle",
        ["SeekingAlpha", "TipRanks", "Topweights"]
    )

    # 4) Ticker hinzufügen
    if st.button("➕ Ticker hinzufügen"):
        tickers = [t.strip() for t in tickers_text.splitlines() if t.strip()]
        if not tickers:
            st.warning("Bitte mindestens einen Ticker eingeben.")
        else:
            added = dm.add_tickers_for_period(
                tickers,
                period_start_date=period_start.strftime("%Y-%m-%d"),
                period_end_date  =period_end.strftime("%Y-%m-%d"),
                source_name      =source
            )
            st.success(f"{len(added)} Ticker hinzugefügt.")

    st.markdown("---")
    st.subheader("Preisdaten aktualisieren")
    if st.button("🔄 Preise updaten"):
        with st.spinner("Lade Preise…"):
            updated = dm.update_ticker_data()
        st.success(f"{len(updated)} Ticker aktualisiert.")

    st.markdown("---")
    # 5) Kontroll-CSV: ticker_info.csv
    info_file    = os.path.expanduser("~/data_alpha/ticker_info.csv")
    periods_file = os.path.expanduser("~/data_alpha/ticker_periods.csv")

    if os.path.exists(info_file):
        st.subheader("Ticker Info")
        st.dataframe(pd.read_csv(info_file), use_container_width=True)
    else:
        st.info("Keine ticker_info.csv gefunden.")

    if os.path.exists(periods_file):
        st.subheader("Ticker Periods")
        st.dataframe(pd.read_csv(periods_file), use_container_width=True)
    else:
        st.info("Keine ticker_periods.csv gefunden.")
