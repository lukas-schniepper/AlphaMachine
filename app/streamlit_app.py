from AlphaMachine_core.db import init_db
init_db()

import streamlit as st
import pandas as pd
import datetime as dt
import tempfile, os

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
page = st.sidebar.radio("🗂️ Seite wählen", ["Backtester", "Data Mgmt"], index=0)

# -----------------------------------------------------------------------------
# 4) CSV-Loader (Session-Cache)
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner="📂 CSV wird geladen…")
def load_csv(file):
    return pd.read_csv(file, index_col=0, parse_dates=True)

# =============================================================================
# === Backtester-UI ===
# =============================================================================
def show_backtester_ui():
    st.sidebar.header("📊 Backtest-Parameter")
    uploaded = st.sidebar.file_uploader("CSV-Preisdaten", type="csv")
    start_balance = st.sidebar.number_input("Startkapital", 10_000, 1_000_000, 100_000, 1_000)
    num_stocks = st.sidebar.slider("Aktien pro Portfolio", 5, 50, 20)

    opt_method = st.sidebar.selectbox(
        "Optimierer", ["ledoit-wolf","minvar","hrp"],
        index=["ledoit-wolf","minvar","hrp"].index(CFG_OPT_METHOD)
    )
    cov_estimator = st.sidebar.selectbox(
        "Kovarianzschätzer", ["ledoit-wolf","constant-corr","factor-model"],
        index=["ledoit-wolf","constant-corr","factor-model"].index(CFG_COV_EST)
    )
    opt_mode = st.sidebar.selectbox(
        "Optimierungsmodus", ["select-then-optimize","optimize-subset"],
        index=["select-then-optimize","optimize-subset"].index(CFG_OPT_MODE)
    )

    rebalance_freq = st.sidebar.selectbox(
        "Rebalance", ["weekly","monthly","custom"],
        index=["weekly","monthly","custom"].index(CFG_REBAL_FREQ)
    )
    custom_months = (
        st.sidebar.slider("Monate zwischen Rebalances",1,12,CFG_CUSTOM_REBAL)
        if rebalance_freq=="custom" else 1
    )
    window_days = st.sidebar.slider("Lookback Days",50,500,CFG_WINDOW,10)

    min_w = st.sidebar.slider("Min Weight (%)",0.0,5.0,float(CFG_MIN_W*100),0.5)/100.0
    max_w = st.sidebar.slider("Max Weight (%)",5.0,50.0,float(CFG_MAX_W*100),1.0)/100.0
    force_eq = st.sidebar.checkbox("Force Equal Weight", CFG_FORCE_EQ)

    st.sidebar.subheader("Trading-Kosten")
    enable_tc = st.sidebar.checkbox("Kosten aktiv", CFG_ENABLE_TC)
    fixed_cost = st.sidebar.number_input("Fixe Kosten pro Trade",0.0,100.0,float(CFG_FIXED_COST))
    var_cost = st.sidebar.number_input("Variable Kosten (%)",0.0,1.0,float(CFG_VAR_COST*100))/100.0

    run_btn = st.sidebar.button("Backtest starten 🚀")

    # Early exit
    if run_btn and uploaded is None:
        st.warning("Bitte zuerst eine CSV-Datei hochladen.")
        st.stop()

    if run_btn and uploaded:
        with st.spinner(f"📈 Backtest läuft…"):
            price_df = load_csv(uploaded)
            if price_df.empty:
                st.error("Hochgeladene CSV enthält keine Daten!")
                st.stop()

            progress = st.progress(0, text="Starte Optimierer…")
            engine = SharpeBacktestEngine(
                price_df, start_balance, num_stocks,
                optimizer_method=opt_method,
                cov_estimator=cov_estimator,
                rebalance_frequency=rebalance_freq,
                custom_rebalance_months=custom_months,
                window_days=window_days,
                min_weight=min_w, max_weight=max_w,
                force_equal_weight=force_eq,
                enable_trading_costs=enable_tc,
                fixed_cost_per_trade=fixed_cost,
                variable_cost_pct=var_cost,
            )
            engine.optimization_mode = opt_mode
            progress.progress(50, text="Rebalancing…")
            engine.run_with_next_month_allocation()
            progress.progress(100, text="Fertig!")

        st.success("Backtest fertig ✅")

        # Tabs
        tabs = st.tabs([
            "Portfolio","Dashboard","Daily","Monthly Allocation",
            "Performance","Risk","Drawdowns","Trading Costs",
            "Rebalance","Selection","Logs"
        ])

        with tabs[0]:
            st.subheader("📈 Portfolio-Verlauf")
            if not engine.portfolio_value.empty:
                st.line_chart(engine.portfolio_value)

        with tabs[1]:
            st.subheader("🔍 KPI-Übersicht")
            if not engine.performance_metrics.empty:
                st.dataframe(engine.performance_metrics, hide_index=True, use_container_width=True)

        with tabs[2]:
            st.subheader("📅 Daily Portfolio")
            if not engine.daily_df.empty:
                st.dataframe(engine.daily_df, use_container_width=True)

        with tabs[3]:
            st.subheader("📊 Monthly Allocation")
            if not engine.monthly_allocations.empty:
                st.dataframe(engine.monthly_allocations, use_container_width=True)

        with tabs[4]:
            st.subheader("📆 Monatliche Performance (%)")
            if not engine.monthly_performance.empty:
                st.bar_chart(
                    engine.monthly_performance.set_index("Date")["Monthly PnL (%)"]
                )

        with tabs[5]:
            st.subheader("⚠️ Risiko")
            st.dataframe(engine.performance_metrics, use_container_width=True)

        with tabs[6]:
            st.subheader("📉 Drawdowns")
            st.dataframe(engine.performance_metrics, use_container_width=True)

        with tabs[7]:
            st.subheader("💸 Trading Costs")
            st.dataframe(engine.performance_metrics, use_container_width=True)

        with tabs[8]:
            st.subheader("🔁 Rebalance Analysis")
            st.dataframe(pd.DataFrame(engine.selection_details), use_container_width=True)

        with tabs[9]:
            st.subheader("🔍 Selection Details")
            st.dataframe(pd.DataFrame(engine.selection_details), use_container_width=True)

        with tabs[10]:
            st.subheader("🪵 Logs")
            st.text("\n".join(engine.log_lines))

        # Excel Download
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir,"AlphaMachine_Report.xlsx")
            export_results_to_excel(engine,tmp_path)
            with open(tmp_path,"rb") as f:
                st.download_button(
                    "📥 Excel-Report",
                    f.read(),
                    file_name=f"AlphaMachine_{dt.date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
    else:
        st.info("Bitte einen Tab wählen und dann Backtest ausführen.")

# =============================================================================
# === Data-Management-UI ===
# =============================================================================
def show_data_ui():
    st.header("📂 Data Management")
    dm = StockDataManager()

    mode = st.radio("Modus", ["➕ Add/Update","👁️ View/Delete"], index=0)

    if mode == "➕ Add/Update":
        st.subheader("➕ Ticker einfügen & Daten updaten")
        tickers = st.text_area("Tickers (eine pro Zeile)", height=120)
        month_dt = st.date_input("Monat wählen", value=dt.date.today().replace(day=1))
        start = month_dt.replace(day=1)
        end   = (pd.to_datetime(start)+pd.offsets.MonthEnd(1)).date()
        st.write(f"Zeitraum: {start} bis {end}")
        source = st.selectbox("Quelle", ["SeekingAlpha","TipRanks","Topweights"])
        if st.button("➕ Hinzufügen"):
            ts = [t.strip() for t in tickers.splitlines() if t.strip()]
            added = dm.add_tickers_for_period(ts,start.strftime("%Y-%m-%d"),end.strftime("%Y-%m-%d"),source)
            st.success(f"{len(added)} Ticker hinzugefügt.")
        if st.button("🔄 Preise updaten"):
            with st.spinner("Aktualisiere…"):
                updated = dm.update_ticker_data()
            st.success(f"{len(updated)} Ticker aktualisiert.")

    else:
        st.subheader("👁️ View/Delete")
        month = st.selectbox("Monat", list({p.start_date.strftime('%Y-%m') for p in dm.get_periods('','')}))
        source = st.selectbox("Quelle", list({p.source for p in dm.get_periods(month,'')}))
        periods = dm.get_periods(month, source)
        if not periods:
            st.info("Keine Einträge.")
        else:
            dfp = pd.DataFrame([vars(p) for p in periods])[['id','ticker','start_date','end_date','source']]
            st.dataframe(dfp.set_index('id'), use_container_width=True)
            to_del = st.multiselect("Zu löschen (ID)", dfp['id'].tolist())
            if st.button("🗑️ Löschen"):
                for pid in to_del:
                    p = session.get(TickerPeriod, pid)
                    session.delete(p)
                session.commit()
                st.success(f"{len(to_del)} Einträge gelöscht.")
                st.experimental_rerun()

        st.markdown("---")
        st.subheader("Ticker Info")
        info = dm.get_ticker_info()
        dfi = pd.DataFrame([vars(i) for i in info])[['id','ticker','sector','currency','actual_start_date','actual_end_date','last_update']]
        col = st.selectbox("Filter-Spalte", dfi.columns.tolist())
        vals = st.multiselect("Filter-Werte", sorted(dfi[col].dropna().unique()))
        if vals:
            dfi = dfi[dfi[col].isin(vals)]
        st.dataframe(dfi.set_index('id'), use_container_width=True)

# -----------------------------------------------------------------------------
# 5) Router
# -----------------------------------------------------------------------------
if page == "Backtester":
    show_backtester_ui()
else:
    show_data_ui()
