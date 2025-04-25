import streamlit as st
import pandas as pd
import io
import datetime as dt

from AlphaMachine_core.engine import SharpeBacktestEngine
from AlphaMachine_core.reporting_no_sparklines import export_results_to_excel

# --- einfacher Passwort-Gate ---------------------------------
pwd = st.sidebar.text_input("Passwort", type="password")
if pwd != st.secrets["APP_PW"]:
    st.warning("🔒 Bitte korrektes Passwort eingeben.")
    st.stop()

# ----------------------------------------------------------------------------
# Page‑Config & Caching
# ----------------------------------------------------------------------------

st.set_page_config("AlphaMachine Backtester", layout="wide")

@st.cache_data(show_spinner="📂 CSV wird geladen…")
def load_csv(file) -> pd.DataFrame:
    """Lädt die CSV einmal pro Session und cached das Ergebnis."""
    return pd.read_csv(file, index_col=0, parse_dates=True)

# ----------------------------------------------------------------------------
# Sidebar – Parameter‑Eingabe
# ----------------------------------------------------------------------------

st.sidebar.header("📊 Backtest‑Parameter")
uploaded = st.sidebar.file_uploader("CSV‑Preisdaten", type="csv")
start_balance = st.sidebar.number_input("Startkapital", 10_000, 1_000_000, 100_000, 1_000)
num_stocks = st.sidebar.slider("Aktien pro Portfolio", 5, 50, 20)
opt_method = st.sidebar.selectbox("Optimierer", ["ledoit-wolf", "minvar", "hrp"])
rebalance_freq = st.sidebar.selectbox("Rebalance", ["monthly", "weekly"])
run_btn = st.sidebar.button("Backtest starten 🚀")

# ----------------------------------------------------------------------------
# Early‑Exit‑Checks
# ----------------------------------------------------------------------------

if run_btn and uploaded is None:
    st.warning("Bitte zuerst eine CSV-Datei hochladen.")
    st.stop()

# ----------------------------------------------------------------------------
# Haupt‑Backtest‑Logik
# ----------------------------------------------------------------------------

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
                rebalance_frequency=rebalance_freq,
            )
            progress.progress(50, text="Rebalancing & Performance…")
            engine.run_with_next_month_allocation()
            progress.progress(100, text="Fertig!")
        except Exception as e:
            st.exception(e)
            st.stop()

    st.success("Backtest fertig ✅")

    # ------------------------------------------------------------------------
    # KPI‑Kacheln
    # ------------------------------------------------------------------------
    if not engine.performance_metrics.empty:
        kpi = engine.performance_metrics.set_index("Metric")["Value"]
        cols = st.columns(4)
        cols[0].metric("CAGR", kpi.get("CAGR (%)", "n/a"))
        cols[1].metric("Sharpe", kpi.get("Sharpe Ratio", "n/a"))
        cols[2].metric("Max DD", kpi.get("Max Drawdown (%)", "n/a"))
        cols[3].metric("Kosten", kpi.get("Trading Costs (% of Initial)", "n/a"))

    # ------------------------------------------------------------------------
    # Portfolio‑Chart
    # ------------------------------------------------------------------------
    if not engine.portfolio_value.empty:
        st.subheader("📈 Portfolio‑Verlauf")
        st.line_chart(engine.portfolio_value)

    # ------------------------------------------------------------------------
    # Excel‑Download
    # ------------------------------------------------------------------------
    buf = io.BytesIO()
    export_results_to_excel(engine, buf)
    st.download_button(
        "📥 Excel‑Report herunterladen",
        data=buf.getvalue(),
        file_name=f"AlphaMachine_{dt.date.today()}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Lade eine CSV hoch, wähle Parameter und starte den Backtest.")
