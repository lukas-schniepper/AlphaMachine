import streamlit as st
import pandas as pd
import datetime as dt
import tempfile
import os

from AlphaMachine_core.engine import SharpeBacktestEngine
from AlphaMachine_core.reporting_no_sparklines import export_results_to_excel
from AlphaMachine_core.config import (
    OPTIMIZER_METHOD as CFG_OPT_METHOD,
    COV_ESTIMATOR as CFG_COV_EST,
    REBALANCE_FREQUENCY as CFG_REBAL_FREQ,
    CUSTOM_REBALANCE_MONTHS as CFG_CUSTOM_REBAL,
    ENABLE_TRADING_COSTS as CFG_ENABLE_TC,
    FIXED_COST_PER_TRADE as CFG_FIXED_COST,
    VARIABLE_COST_PCT as CFG_VAR_COST,
)

# ------------------------------------------------------------
# 1) Page‑Config → MUSS die **erste** Streamlit‑Anweisung sein
# ------------------------------------------------------------

st.set_page_config("AlphaMachine Backtester", layout="wide")

# ------------------------------------------------------------
# 2) Passwort‑Gate (Secrets)
# ------------------------------------------------------------

pwd = st.sidebar.text_input("Passwort", type="password")
if pwd != st.secrets["APP_PW"]:
    st.warning("🔒 Bitte korrektes Passwort eingeben.")
    st.stop()

# ------------------------------------------------------------
# 3) CSV‑Loader (Session‑Cache)
# ------------------------------------------------------------

@st.cache_data(show_spinner="📂 CSV wird geladen…")
def load_csv(file):
    return pd.read_csv(file, index_col=0, parse_dates=True)

# ------------------------------------------------------------
# 4) Sidebar – Parameter‑Eingabe
# ------------------------------------------------------------

st.sidebar.header("📊 Backtest‑Parameter")
uploaded          = st.sidebar.file_uploader("CSV‑Preisdaten", type="csv")
start_balance     = st.sidebar.number_input("Startkapital", 10_000, 1_000_000, 100_000, 1_000)
num_stocks        = st.sidebar.slider("Aktien pro Portfolio", 5, 50, 20)
opt_method        = st.sidebar.selectbox("Optimierer", ["ledoit-wolf", "minvar", "hrp"], index=["ledoit-wolf","minvar","hrp"].index(CFG_OPT_METHOD))
cov_estimator     = st.sidebar.selectbox("Kovarianzschätzer", ["ledoit-wolf", "constant-corr", "factor-model"], index=["ledoit-wolf","constant-corr","factor-model"].index(CFG_COV_EST))
rebalance_freq    = st.sidebar.selectbox("Rebalance", ["weekly", "monthly", "custom"], index=["weekly","monthly","custom"].index(CFG_REBAL_FREQ))
custom_months     = 1
if rebalance_freq == "custom":
    custom_months = st.sidebar.slider("Monate zwischen Rebalances", 1, 12, CFG_CUSTOM_REBAL)

# Trading‑Kosten
st.sidebar.subheader("Trading‑Kosten")
enable_tc   = st.sidebar.checkbox("Kosten aktiv", CFG_ENABLE_TC)
fixed_cost  = st.sidebar.number_input("Fixe Kosten pro Trade", 0.0, 100.0, float(CFG_FIXED_COST))
var_cost    = st.sidebar.number_input("Variable Kosten (%)", 0.0, 1.0, float(CFG_VAR_COST*100.0)) / 100.0

run_btn = st.sidebar.button("Backtest starten 🚀")

# ------------------------------------------------------------
# 5) Early‑Exit‑Checks
# ------------------------------------------------------------

if run_btn and uploaded is None:
    st.warning("Bitte zuerst eine CSV-Datei hochladen.")
    st.stop()

# ------------------------------------------------------------
# 6) Haupt‑Backtest‑Logik
# ------------------------------------------------------------

if run_btn and uploaded:
    with st.spinner(f"📈 Backtest für {uploaded.name} läuft…"):
        price_df = load_csv(uploaded)
        if price_df.empty:
            st.error("Die hochgeladene CSV enthält keine Daten!")
            st.stop()

        progress = st.progress(0, text="Starte Optimierer…")
        try:
            engine = SharpeBacktestEngine(
                price_df,
                start_balance,
                num_stocks,
                optimizer_method=opt_method,
                cov_estimator=cov_estimator,
                rebalance_frequency=rebalance_freq,
                custom_rebalance_months=custom_months,
                enable_trading_costs=enable_tc,
                fixed_cost_per_trade=fixed_cost,
                variable_cost_pct=var_cost,
            )
            progress.progress(50, text="Rebalancing & Performance…")
            engine.run_with_next_month_allocation()
            progress.progress(100, text="Fertig!")
        except Exception as e:
            st.exception(e)
            st.stop()

    st.success("Backtest fertig ✅")

    # KPI‑Kacheln
    if not engine.performance_metrics.empty:
        kpi = engine.performance_metrics.set_index("Metric")["Value"]
        cols = st.columns(4)
        cols[0].metric("CAGR", kpi.get("CAGR (%)", "n/a"))
        cols[1].metric("Sharpe", kpi.get("Sharpe Ratio", "n/a"))
        cols[2].metric("Max DD", kpi.get("Max Drawdown (%)", "n/a"))
        cols[3].metric("Kosten", kpi.get("Trading Costs (% of Initial)", "n/a"))

    # Portfolio‑Chart
    if not engine.portfolio_value.empty:
        st.subheader("📈 Portfolio‑Verlauf")
        st.line_chart(engine.portfolio_value)

    # Excel‑Download (Temp‑File, um Korrupte‑Warnung zu vermeiden)
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = os.path.join(tmp_dir, "AlphaMachine_Report.xlsx")
        export_results_to_excel(engine, tmp_path)
        with open(tmp_path, "rb") as f:
            st.download_button(
                "📥 Excel‑Report",
                f.read(),
                file_name=f"AlphaMachine_{dt.date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
else:
    st.info("Lade eine CSV hoch, wähle Parameter und starte den Backtest.")
