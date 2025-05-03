import streamlit as st
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
import datetime as dt
import tempfile, os
from sqlmodel import select
import plotly.graph_objects as go
from AlphaMachine_core.models import TickerPeriod
from AlphaMachine_core.db import init_db, get_session
from AlphaMachine_core.optimize_params import run_optimizer

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
def load_price_df(month, sources, start_date, end_date):
    dm = StockDataManager()
    tickers = dm.get_tickers_for(month, sources)
    raw = dm.get_price_data(
        tickers,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )
    if not raw:
        return pd.DataFrame()

    return (
        pd.DataFrame([r.model_dump() for r in raw])
          .assign(date=lambda d: pd.to_datetime(d["trade_date"]))
          .pivot(index="date", columns="ticker", values="close")
          .sort_index()
    )



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

    # ### OPTIMIZER START – Sidebar‑Widgets  ###
    st.sidebar.markdown("---")
    st.sidebar.header("🚀 Optimizer")

    kpi_weights = {
        "Sharpe Ratio": st.sidebar.slider("Sharpe‑Gewicht", 0.0, 3.0, 1.0, 0.1),
        "Ulcer Index":  -st.sidebar.slider("Ulcer‑Gewicht",  0.0, 3.0, 1.0, 0.1),
        "CAGR (%)":     st.sidebar.slider("CAGR‑Gewicht",   0.0, 3.0, 1.0, 0.1),
    }

    opt_trials  = st.sidebar.number_input("Versuche", 10, 500, 50, 10)
    
    # --- Buttons -----------------------------------------------
    run_opt_btn = st.sidebar.button("Optimizer starten 🚀")
    run_btn     = st.sidebar.button("Backtest starten 🚀")

    # Wenn *keiner* gedrückt wurde → zurück
    if not run_btn and not run_opt_btn:
        st.info("Stelle alle Parameter ein und klicke auf einen der Start‑Buttons.")
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
          .assign(date=lambda df: pd.to_datetime(df["trade_date"]))
          .pivot(index="date", columns="ticker", values="close")
          .sort_index()
    )

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
            optimization_mode=opt_mode,
        )

        engine.universe_mode = "static" if mode.startswith("statisch") else "dynamic"

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

        engine.run_with_next_month_allocation()

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
        st.subheader("🔍 KPI-Übersicht")
        if not engine.performance_metrics.empty:
            st.dataframe(engine.performance_metrics, hide_index=True, use_container_width=True)

        st.markdown("---")
        st.subheader("📈 Portfolio-Verlauf")
        if not engine.portfolio_value.empty:
            st.line_chart(engine.portfolio_value)

        st.markdown("---")
        st.subheader("📆 Monatliche Performance (%)")
        if not engine.monthly_performance.empty:
            st.bar_chart(
                engine.monthly_performance.set_index("Date")["Monthly PnL (%)"]
            )

    with tabs[1]:
        st.subheader("📅 Daily Portfolio")
        if not engine.daily_df.empty:
            st.dataframe(engine.daily_df, use_container_width=True)

    with tabs[2]:
        st.subheader("🗓️ Monthly Performance Detail")
        if not engine.price_data.empty and not engine.portfolio_value.empty:
            # 1) Monats-Endkurse und Monats-End-Portfolio-Wert
            monthly_prices  = engine.price_data.resample("ME").last()
            monthly_balance = engine.portfolio_value.resample("ME").last()

            # 2) Index-Namen setzen, damit reset_index() eine Spalte "Date" erzeugt
            monthly_prices.index.name  = "Date"
            monthly_balance.index.name = "Date"

            # 3) Monatsrenditen in Prozent
            monthly_returns = monthly_prices.pct_change().dropna() * 100

            # 4) Portfolio-Monatsrendite aus engine.monthly_performance
            port_rets = (
                engine.monthly_performance
                    .set_index("Date")["Monthly PnL (%)"]
            )

            # 5) alles in ein DataFrame packen
            df = monthly_returns.copy()
            df["Balance"]    = monthly_balance
            df["Return (%)"] = port_rets

            # 6) Index in Spalte umwandeln – jetzt gibt es garantiert eine Spalte "Date"
            df = df.reset_index()

            # 7) Jahr und Monatsname aus "Date" ableiten
            df["Year"]  = df["Date"].dt.year
            df["Month"] = df["Date"].dt.month_name()

            # 8) **WICHTIG**: zuerst nach Date absteigend sortieren
            df = df.sort_values("Date", ascending=False).reset_index(drop=True)

            # 9) dann die finalen Spalten in der gewünschten Reihenfolge auswählen
            cols = ["Year", "Month", "Return (%)", "Balance"] + list(monthly_returns.columns)
            df = df[cols]

            # 10) Formatierung: Prozent-Spalten mit 1 Dezimalstelle und Prozentzeichen
            percent_cols = ["Return (%)"] + list(monthly_returns.columns)
            fmt = {
                **{c: "{:.1f}%" for c in percent_cols},   # Prozent-Spalten mit 1 Dezimalstelle + '%'
                "Balance": "{:,.0f}"                      # Balance ohne Dezimalstellen, Tausender-Komma
            }
            styled = df.style.format(fmt)

            st.dataframe(styled, use_container_width=True)

        else:
            st.info("Keine Daten für Monthly Performance.")

    with tabs[3]:
        st.subheader("🗓️ Yearly Performance Detail")
        if not engine.price_data.empty and not engine.portfolio_value.empty:
            # 1) Jahr-Endkurse und Jahr-End-Portfolio-Wert
            yearly_prices  = engine.price_data.resample("YE").last()
            yearly_balance = engine.portfolio_value.resample("YE").last()

            # 2) Index benennen, damit reset_index eine Date-Spalte erzeugt
            yearly_prices.index.name  = "Date"
            yearly_balance.index.name = "Date"

            # 3) Jahres-Renditen in Prozent für alle Ticker
            yearly_returns = yearly_prices.pct_change().dropna() * 100

            # 4) Portfolio-Jahresrendite
            port_year_rets = yearly_balance.pct_change().dropna() * 100

            # 5) DataFrame zusammenbauen
            df_year = yearly_returns.copy()
            df_year["Balance"]    = yearly_balance
            df_year["Return (%)"] = port_year_rets

            # 6) Index in Spalte umwandeln
            df_year = df_year.reset_index()

            # 7) Year aus der Date-Spalte
            df_year["Year"] = df_year["Date"].dt.year

            # 8) Spalten in gewünschter Reihenfolge
            cols = ["Year", "Return (%)", "Balance"] + list(yearly_returns.columns)
            df_year = df_year[cols]

            # 9) Neueste Jahre zuerst
            df_year = df_year.sort_values("Year", ascending=False).reset_index(drop=True)

            # 10) Prozentformatierung auf 1 Dezimalstelle
            percent_cols = ["Return (%)"] + list(yearly_returns.columns)
            fmt = {
                **{c: "{:.1f}%" for c in percent_cols},     # Percent columns
                "Balance": "{:,.0f}"                        # Balance: no decimals, comma as thousands separator
            }
            styled = df_year.style.format(fmt)

            st.dataframe(styled, use_container_width=True)
        else:
            st.info("Keine Daten für Yearly Performance.")


    with tabs[4]:
        st.subheader("📊 Monthly Allocation")
        if not engine.monthly_allocations.empty:
            df_sorted = engine.monthly_allocations.sort_values(
                by="Rebalance Date",
                ascending=False
            )
            st.dataframe(df_sorted, use_container_width=True)

    with tabs[5]:
        # hole das letzte Datum aus engine.portfolio_value (oder price_data)
        last_date = engine.price_data.index.max()

        # bestimme den aktuellen Monat und addiere 1
        next_period = last_date.to_period("M") + 1

        # formatiere Anf- und Enddatum
        start = next_period.to_timestamp(how="start").strftime("%d. %B %Y")
        end   = next_period.to_timestamp(how="end").strftime("%d. %B %Y")

        # Anzeige in Deinem Tab:
        st.subheader("🔮 Next Month Allocation")
        st.markdown(f"**Zeitraum:** {start} – {end}")

        if hasattr(engine, "next_month_weights"):
            df_next = (
                engine.next_month_weights
                    .mul(100)               # in Prozent
                    .reset_index()
            )
            df_next.columns = ["Ticker","Gewicht (%)"]
            st.dataframe(df_next, use_container_width=True)
        else:
            st.info("Keine Auswahl für den Folgemonat (zu wenige Daten).")

    with tabs[6]:
        st.subheader("📉 Top 10 Drawdowns")
        # Drawdown-Berechnung
        df_port = engine.portfolio_value.to_frame(name="Portfolio")
        df_port["Peak"] = df_port["Portfolio"].cummax()
        df_port["Drawdown"] = df_port["Portfolio"] / df_port["Peak"] - 1

        # Drawdown-Episoden extrahieren
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
                    # Drawdown abgeschlossen
                    periods.append({
                        "Start":            start.date(),
                        "Trough":           trough.date(),
                        "End":              date.date(),
                        "Length (Days)":    (date - start).days,
                        "Recovery Time":    (date - trough).days,
                        "Drawdown (%)":     round((trough_val/peak_val - 1)*100, 2),
                    })
                    in_dd = False

        # laufende DD-Periode (falls nicht abgeschlossen)
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

        # Top 10 sortiert nach Drawdown‐Size
        df_dd = (
            pd.DataFrame(periods)
            .sort_values(by="Drawdown (%)")  # drawdowns sind negativ, also aufsteigend = größter Drawdown zuerst
            .head(10)
            .reset_index(drop=True)
        )

        # Spaltenreihenfolge und Header anpassen
        df_dd = df_dd[
            ["Start", "End", "Length (Days)", "Recovery Time", "Trough", "Drawdown (%)"]
        ]
        df_dd.columns = [
            "Start", "End", "Length", "Recovery Time", "Underwater Period", "Drawdown"
        ]

        st.dataframe(df_dd, use_container_width=True)

    # Tab 5: Trading Costs
    with tabs[7]:
        st.subheader("💸 Trading Costs")
        if not engine.monthly_allocations.empty and "Trading Costs" in engine.monthly_allocations:
            cost_df = (
                engine.monthly_allocations
                    .dropna(subset=["Trading Costs"])
                    .groupby("Rebalance Date")["Trading Costs"]
                    .sum()
                    .reset_index(name="Total Trading Costs")
            )
            st.dataframe(cost_df, use_container_width=True)
        else:
            st.info("Keine Trading-Kosten-Daten vorhanden.")

    # Tab 6: Rebalance Analysis
    with tabs[8]:
        st.subheader("🔁 Rebalance Analysis")
        df_reb = pd.DataFrame(engine.selection_details)
        # nur echte Rebalances, keine SUMMARY-Zeile
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
        for line in engine.ticker_coverage_logs + engine.log_lines:
            st.text(line)

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
    st.header("⚙️ Hyperparameter‑Optimizer")

    # ---------- Daten‑Selektion ------------------------------------
    dm      = StockDataManager()
    month   = st.selectbox("Start‑Monat (Universe)", dm.get_periods_distinct_months())
    sources = st.multiselect("Quellen", ["Topweights", "SeekingAlpha", "TipRanks"], ["Topweights"])
    col1, col2 = st.columns(2)
    start_date = col1.date_input("Backtest‑Start", value=dt.date.today()-dt.timedelta(days=5*365))
    end_date   = col2.date_input("Backtest‑Ende",  value=dt.date.today(), min_value=start_date)

    price_df = load_price_df(month, sources, start_date, end_date)
    if price_df.empty:
        st.warning("⚠️ Keine Preisdaten gefunden.")
        st.stop()

    # ---------- Suchraum‑Editor ------------------------------------
    PARAMS = {                         # label, *num‑range OR list
        "num_stocks":         ("Anzahl Aktien", 5, 50, 1),
        "window_days":        ("Lookback Tage", 50, 500, 10),
        "min_weight":         ("Min‑Weight %", 0.0, 5.0, 0.5),
        "max_weight":         ("Max‑Weight %", 5.0, 50.0, 1.0),
        "force_equal_weight": ("Equal‑Weight", [False, True]),
        "optimization_mode":  ("Mode", ["select-then-optimize", "optimize-subset"]),
        "optimizer_method":   ("Optimizer", ["ledoit-wolf", "minvar", "hrp"]),
        "cov_estimator":      ("Cov‑Estimator", ["ledoit-wolf", "constant-corr", "factor-model"]),
    }

    search_space = {}
    with st.expander("🔧 Suchraum definieren", expanded=True):
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
                sel  = st.multiselect(f"{label} – Kandidaten", opts, opts, key=f"ms_{key}")
                search_space[key] = ("categorical", sel)

    st.info(f"🎯 Aktueller Suchraum:  {search_space}")

    # ──────────────────────────────────────────────────────────────
    # NEU ▸ Defaults, falls eine Variable NICHT optimiert wird
    # ──────────────────────────────────────────────────────────────
    base_num_stocks = None
    if "num_stocks" not in search_space:
        base_num_stocks = st.number_input(
            "Anzahl Aktien (fix – wenn nicht optimiert)",
            min_value=5, max_value=50, value=20, step=1, key="fix_num"
        )

    base_window_days = None
    if "window_days" not in search_space:
        base_window_days = st.slider(
            "Lookback Tage (fix – wenn nicht optimiert)",
            min_value=50, max_value=500, value=200, step=10, key="fix_win"
        )

    # ---------- KPI‑Gewichte & Trials --------------------------------
    with st.expander("🎯 Objective‑Gewichte"):
        kpi_weights = {
            "Sharpe Ratio": st.slider("Sharpe", 0.0, 3.0, 1.0, 0.1),
            "Ulcer Index": -st.slider("Ulcer Index", 0.0, 3.0, 1.0, 0.1),
            "CAGR (%)":     st.slider("CAGR", 0.0, 3.0, 1.0, 0.1),
        }

    n_trials = st.number_input("Trials", 10, 500, 100, 10)

    # ---------- Fixed Args (Engine) ----------------------------------
    fixed_kwargs = dict(
        start_balance = 100_000,
        start_month   = month,
        universe_mode = "static",
        rebalance_frequency = "monthly",
        custom_rebalance_months = 1,
        enable_trading_costs = False,
    )

    # ▸ Pflicht‑Parameter nur setzen, wenn sie NICHT im Suchraum sind
    if "num_stocks"  not in search_space:
        fixed_kwargs["num_stocks"] = base_num_stocks
    if "window_days" not in search_space:
        fixed_kwargs["window_days"] = base_window_days

    if st.button("🚀 Suche starten"):
        study = run_optimizer(price_df, fixed_kwargs, search_space, kpi_weights, n_trials)
        show_study_results(study, kpi_weights, price_df, fixed_kwargs)


def show_study_results(
    study,
    kpi_weights: dict[str, float],
    price_df: pd.DataFrame,
    fixed_kwargs: dict,
):
    import re, pandas as pd
    from AlphaMachine_core.engine import SharpeBacktestEngine

    # -------  A) Trials‑DataFrame aufbereiten  -----------------------
    df = study.trials_dataframe()

    # Optuna ≥ 4 → MultiIndex flatten
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            sec if main in ("params", "user_attrs") else main
            for main, sec in df.columns.to_list()
        ]

    # Optuna ≤ 3 → user_attrs‑Dict aufspalten
    if "user_attrs" in df.columns:
        df = pd.concat(
            [df.drop(columns=["user_attrs"]), df["user_attrs"].apply(pd.Series)],
            axis=1
        )

    # Präfixe entfernen
    df = df.rename(columns=lambda c: re.sub(r"^(param_|params_|user_attrs?_)", "", c))

    kpi_map  = {"Sharpe Ratio": "Sharpe", "CAGR (%)": "CAGR", "Ulcer Index": "Ulcer Index"}
    kpi_cols = [kpi_map[k] for k in kpi_weights if kpi_map[k] in df.columns]

    # -------  B) TOP‑10‑Tabelle  -------------------------------------
    cols_top = ["number", "value"] + kpi_cols + [
        c for c in sorted(df.columns) if c not in ("number", "value", *kpi_cols)
    ]

    st.subheader("🏆 Top 10 Runs")
    st.dataframe(
        df[cols_top].sort_values("value", ascending=False).head(10).style.hide(axis="index"),
        use_container_width=True,
    )

    # -------  C) Best‑Run erneut ausführen  --------------------------
    best_params = study.best_params
    kwargs      = {**fixed_kwargs, **best_params}

    # falls Optuna diesen Pflicht‑Param nicht optimiert hat
    if "num_stocks"  not in kwargs:
        kwargs["num_stocks"]  = fixed_kwargs.get("num_stocks", 20)
    if "window_days" not in kwargs:
        kwargs["window_days"] = fixed_kwargs.get("window_days", 200)

    eng = SharpeBacktestEngine(price_df, **kwargs)
    eng.run_with_next_month_allocation()

    # -------  D) Detail‑Tabellen  ------------------------------------
    best = df.loc[df["value"].idxmax()]
    param_cols = [c for c in best.index if c not in ("number", "value", *kpi_cols)]

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 🥇 Best‑Run KPIs")
        st.table(best[kpi_cols].rename_axis("KPI").to_frame("Wert"))
    with col2:
        st.markdown("### ⚙️ Best‑Run Parameter")
        st.table(best[param_cols].dropna().rename_axis("Parameter").to_frame("Wert"))

    # ---------  E) Performance & Balance pro Jahr  -------------------
    st.markdown("### 📈 Performance & Balance pro Jahr")

    yearly_bal      = eng.portfolio_value.resample("YE").last()
    yearly_ret_pct  = yearly_bal.pct_change().mul(100).round(1)   # <-- FIX

    df_year = pd.DataFrame({
        "Year":        yearly_bal.index.year,
        "Return (%)":  yearly_ret_pct,
        "Balance":     yearly_bal.round(0).astype(int),
    }).dropna().astype({"Year": int}).reset_index(drop=True)

    st.table(df_year)



# -----------------------------------------------------------------------------
# 5) Router
# -----------------------------------------------------------------------------
if page == "Backtester":
    show_backtester_ui()
elif page == "Optimizer":
    show_optimizer_ui()
else:
    show_data_ui()
