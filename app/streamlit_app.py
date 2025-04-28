import streamlit as st
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
import datetime as dt
import tempfile, os
from sqlmodel import select
import plotly.graph_objects as go
from pandas.tseries.offsets import BDay
from AlphaMachine_core.models import TickerPeriod
from AlphaMachine_core.db import init_db, get_session

init_db() 

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
    dm = StockDataManager()

    # - 0) Backtest Periode festlegen
    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input(
        "Backtest-Startdatum",
        value=dt.date.today() - dt.timedelta(days=365),
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

    # — 1) Quellen-Auswahl (DB + Defaults) —
    with get_session() as session:
        existing = session.exec(select(TickerPeriod.source)).all()
    defaults = ["SeekingAlpha", "TipRanks", "Topweights"]
    sources = st.sidebar.multiselect(
        "Datenquellen auswählen",
        options=sorted(set(existing + defaults)),
        default=["Topweights"]
    )

    # — 2) Monat wählen —
    months = dm.get_periods_distinct_months()
    month  = st.sidebar.selectbox("Periode wählen (YYYY-MM)", months)

    # — 3) Modus: statisch vs. dynamisch —
    mode = st.sidebar.radio(
        "Ticker-Universe",
        ["statisch (gesamte Periode)", "dynamisch (monatlich)"]
    )

    # — 4) Lookback Days (Backtest-Fenster) —
    window_days = st.sidebar.slider(
        "Lookback Days", 
        min_value=50,
        max_value=500,
        value=CFG_WINDOW,   # comes from your config
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

    # — 8) Backtest auslösen —
    run_btn = st.sidebar.button("Backtest starten 🚀")
    if not run_btn:
        st.info("Stelle alle Parameter in der Sidebar ein und klicke auf „Backtest starten“.")
        return

    # — VALIDIERUNG —
    if not sources:
        st.error("Bitte mindestens eine Quelle auswählen."); return
    if not month:
        st.error("Bitte einen Monat auswählen."); return

    # — 9) Ticker + PriceData laden + Pivot … und Backtest laufen lassen —
    tickers = dm.get_tickers_for(month, sources)

    #DEBUG
    #st.write(f"🔎 got {len(tickers)} tickers:", tickers)

    if not tickers:
        st.error("Keine Ticker für diese Auswahl."); return

    end = dt.date.today()
    history_start = (
        (end - dt.timedelta(days=window_days)).strftime("%Y-%m-%d")
        if mode.startswith("statisch")
        else f"{month}-01"
    )
    # Statt history_start / month-Logik:
    raw = dm.get_price_data(
        tickers,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )

    #DEBUG
    #st.write(f"🔎 got {len(raw)} raw price records")

    #if raw:
    #    st.write(raw[:3])  # oder raw[0].model_dump() / raw[0].dict()
    #------------------------

    if not raw:
        st.error("Keine Preisdaten gefunden."); return

    price_df = (
        pd.DataFrame([r.model_dump() for r in raw])
          .assign(date=lambda df: pd.to_datetime(df["date"]))
          .pivot(index="date", columns="ticker", values="close")
          .sort_index()
    )

    #DEBUG
    #st.write("🔎 price_df shape:", price_df.shape)
    #st.write(price_df.head())
    #----------------------

    with st.spinner("📈 Backtest läuft…"):
        engine = SharpeBacktestEngine(
            price_df,
            start_balance,
            num_stocks,
            start_month=month,
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
        )

        engine.universe_mode = "static" if mode.startswith("statisch") else "dynamic"

        #DEBUG
        #st.write(f"🔎 running backtest on {price_df.shape[0]} days × {price_df.shape[1]} tickers")
        #----------------------

        engine.run_with_next_month_allocation()

        #DEBUG
        #st.write("🔎 final portfolio_value:", engine.portfolio_value.tail())
        #st.write("🔎 performance_metrics:", engine.performance_metrics)
        #------------------------

    st.success("Backtest fertig ✅")

    # Tabs
    tabs = st.tabs([
        "Dashboard","Portfolio","Daily","Monthly Allocation",
        "Performance","Risk","Drawdowns","Trading Costs",
        "Rebalance","Selection","Logs"
    ])

    with tabs[0]:
        st.subheader("🔍 KPI-Übersicht")
        if not engine.performance_metrics.empty:
            st.dataframe(engine.performance_metrics, hide_index=True, use_container_width=True)


    with tabs[1]:
        st.subheader("📈 Portfolio-Verlauf")
        if not engine.portfolio_value.empty:
            st.line_chart(engine.portfolio_value)


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
        defaults = ["SeekingAlpha","TipRanks","Topweights"]
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
    
    records = [{"date":r.date, "open":r.open, "high":r.high, "low":r.low, "close":r.close, "volume":r.volume} for r in raw]
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
# 5) Router
# -----------------------------------------------------------------------------
if page == "Backtester":
    show_backtester_ui()
else:
    show_data_ui()
