import pandas as pd
import warnings
import numpy as np
from AlphaMachine_core.optimizers import optimize_portfolio
from AlphaMachine_core.utils import (
    build_rebalance_schedule,
    select_top_sharpe_tickers,
    allocate_positions,
)
from AlphaMachine_core.risk_overlay.overlay import RiskOverlay
from AlphaMachine_core import config as CFG

warnings.filterwarnings(
    "ignore",
    message="Values in x were outside bounds during a minimize step, clipping to bounds",
    module="scipy.optimize._slsqp_py"
)

from AlphaMachine_core.config import (
    BACKTEST_WINDOW_DAYS,
    MIN_WEIGHT,
    MAX_WEIGHT,
    REBALANCE_FREQUENCY,
    OPTIMIZE_WEIGHTS,
    OPTIMIZER_METHOD,
    COV_ESTIMATOR,
    OPTIMIZATION_MODE,
    FORCE_EQUAL_WEIGHT,
    CUSTOM_REBALANCE_MONTHS,
    ENABLE_TRADING_COSTS,
    FIXED_COST_PER_TRADE,
    VARIABLE_COST_PCT,
)


class SharpeBacktestEngine:
    def __init__(
        self,
        price_data: pd.DataFrame,
        start_balance: float,
        num_stocks: int,
        start_month: str,
        universe_mode: str = "static",
        optimize_weights=None,
        optimizer_method=None,
        cov_estimator=None,
        min_weight: float = MIN_WEIGHT,
        max_weight: float = MAX_WEIGHT,
        window_days: int = BACKTEST_WINDOW_DAYS,
        force_equal_weight: bool = FORCE_EQUAL_WEIGHT,
        rebalance_frequency: str = REBALANCE_FREQUENCY,
        custom_rebalance_months: int = CUSTOM_REBALANCE_MONTHS,
        enable_trading_costs: bool = ENABLE_TRADING_COSTS,
        fixed_cost_per_trade: float = FIXED_COST_PER_TRADE,
        variable_cost_pct: float = VARIABLE_COST_PCT,
        optimization_mode: str = OPTIMIZATION_MODE,
    ):
        self.user_start_date = pd.to_datetime(start_month)
        # User-gewünschtes Startdatum speichern
        all_dates = price_data.index

        # Finde Index des gewünschten Startdatums (oder nächsten Handelstag)
        if self.user_start_date in all_dates:
            user_start_idx = all_dates.get_loc(self.user_start_date)
        else:
            # Nimm den nächsten verfügbaren Handelstag nach user_start_date
            user_start_idx = all_dates.get_indexer([self.user_start_date], method="backfill")[0]
            if user_start_idx == -1:
                raise ValueError("User-Startdatum liegt nach den Preisdaten!")

        # Berechne den Index, ab dem geladen werden soll
        lookback_start_idx = max(0, user_start_idx - window_days - 5)  # 5 Tage Puffer

        # Effektiver Startzeitpunkt
        self.effective_start_date = all_dates[lookback_start_idx]

        # Lade die Preisdaten ab diesem Zeitpunkt
        self.price_data = price_data.loc[self.effective_start_date:]
        
        # 3. Restliche unveränderliche Kern-Parameter
        self.start_balance   = start_balance
        self.num_stocks      = num_stocks

        # ➋ Universe-Mode speichern
        self.universe_mode   = universe_mode.lower()

        # ➌ Coverage-Filter nur im dynamischen ("dynamic") Mode anwenden
        if self.universe_mode == "dynamic":
            self._filter_complete_tickers()
        else:
            # Im static Mode behalten wir alle übergebenen Ticker
            self.filtered_tickers          = []
            self.filtered_tickers_by_month = {}
            self.monthly_filtered_report   = []

        # ➍ Leere DataFrames / Listen initialisieren
        self.portfolio_value    = pd.Series(dtype=float)
        self.daily_df           = pd.DataFrame()
        self.monthly_allocations= pd.DataFrame()
        self.selection_details  = []
        self.log_lines          = []
        self.ticker_coverage_logs = []
        self.missing_months     = []
        self.performance_metrics= pd.DataFrame()
        self.monthly_performance= pd.DataFrame()
        self.total_trading_costs= 0.0

        # ➎ Optimierungs- & Rebalance-Parameter setzen
        self.optimize_weights       = optimize_weights if optimize_weights is not None else OPTIMIZE_WEIGHTS
        self.optimizer_method       = optimizer_method   if optimizer_method   is not None else OPTIMIZER_METHOD
        self.cov_estimator          = cov_estimator      if cov_estimator      is not None else COV_ESTIMATOR
        self.min_weight             = min_weight
        self.max_weight             = max_weight
        self.window_days            = window_days
        self.force_equal_weight     = force_equal_weight
        self.rebalance_freq         = rebalance_frequency
        self.custom_rebalance_months= custom_rebalance_months
        self.enable_trading_costs   = enable_trading_costs
        self.fixed_cost_per_trade   = fixed_cost_per_trade
        self.variable_cost_pct      = variable_cost_pct
        self.optimization_mode      = optimization_mode if optimization_mode is not None else OPTIMIZATION_MODE

       
        # Cash Handling
        self.current_cash = self.start_balance

        # RiskOverlay-Objekt initialisieren
        if CFG.RISK_OVERLAY["enabled"]:
            self.risk_overlay = RiskOverlay(CFG.RISK_OVERLAY["config_path"])
        else:
            self.risk_overlay = None

        print("Preis-DataFrame MIN:", price_data.index.min())
        print("Preis-DataFrame MAX:", price_data.index.max())
        print("Erwarteter effektiver Start:", self.effective_start_date)
        print("Gewünschter User-Start:", self.user_start_date)


    def _get_valid_tickers(self, threshold=0.95):
        full_range = pd.date_range(
            start=self.price_data.index.min(), end=self.price_data.index.max(), freq="B"
        )
        min_coverage_days = int(len(full_range) * threshold)
        valid = []
        invalid = []
        missing_by_month = {}

        for col in self.price_data.columns:
            print(col, self.price_data[col].first_valid_index())
            col_data = self.price_data[col].dropna()
            col_dates = col_data.index
            coverage = len(col_dates)
            coverage_pct = coverage / len(full_range) * 100

            if coverage >= min_coverage_days:
                valid.append(col)
                self.ticker_coverage_logs.append(
                    f"📈 {col} | {col_dates.min().date()}–{col_dates.max().date()} | {coverage}/{len(full_range)} ({coverage_pct:.1f}%)"
                )
            else:
                invalid.append(
                    {
                        "Ticker": col,
                        "Start Date": (
                            col_dates.min().date() if not col_dates.empty else None
                        ),
                        "End Date": (
                            col_dates.max().date() if not col_dates.empty else None
                        ),
                        "Coverage": f"{coverage}/{len(full_range)} ({coverage_pct:.1f}%)",
                    }
                )
                self.ticker_coverage_logs.append(
                    f"❌ {col} | {coverage}/{len(full_range)} days ({coverage_pct:.1f}%)"
                )
                for date in full_range[~full_range.isin(col_dates)]:
                    month = date.strftime("%Y-%m")
                    missing_by_month.setdefault(month, {}).setdefault(col, 0)
                    missing_by_month[month][col] += 1

        return valid, invalid, missing_by_month

    def _filter_complete_tickers(self):
        print("🔍 Filtering tickers with at least 95% data coverage...")
        self.ticker_coverage_logs.append(
            "🔍 Filtering tickers with at least 95% data coverage..."
        )
        valid, invalid, missing = self._get_valid_tickers()
        self.filtered_tickers = invalid
        self.filtered_tickers_by_month = missing
        self.monthly_filtered_report = [
            {"Month": m, "Ticker": t, "Missing Days": d}
            for m, tickers in missing.items()
            for t, d in tickers.items()
        ]
        print(f"✅ {len(valid)} tickers retained out of {self.price_data.shape[1]}")
        print(f"❌ {len(invalid)} tickers filtered out")
        self.ticker_coverage_logs.append(
            f"✅ {len(valid)} tickers retained out of {self.price_data.shape[1]}"
        )
        self.ticker_coverage_logs.append(f"❌ {len(invalid)} tickers filtered out")
        self.price_data = self.price_data[valid]

    def run_with_next_month_allocation(self, top_universe_size: int = 100):

        """
        Führt den Backtest aus, erlaubt bei jedem Rebalance auch weniger als
        self.num_stocks verfügbare Ticker (setzt n_stocks = available).
        """

        # 1) Renditen berechnen und vollständig leere Zeilen entfernen
        returns = self.price_data.pct_change().dropna(how="all")

        # **NEU**: Alle verbliebenen NaNs durch 0 ersetzen,
        # damit LedoitWolf & Co. sauber rechnen können
        returns = returns.fillna(0)
  

        # 2) Rebalance-Zeitplan
        rebalance_schedule = build_rebalance_schedule(
            self.price_data,
            frequency=self.rebalance_freq,
            custom_months=self.custom_rebalance_months,
        )

        # Finde ersten Rebalance-Zeitpunkt, für den genug Handelstage vorhanden sind
        returns_idx = returns.index
        first_valid_idx = None
        for i, entry in enumerate(rebalance_schedule):
            rebalance_date = entry["end_date"]
            if rebalance_date not in returns_idx:
                # Nimm den letzten verfügbaren Handelstag VOR oder GLEICH rebalance_date
                past_dates = returns_idx[returns_idx <= rebalance_date]
                if len(past_dates) == 0:
                    continue
                rebalance_date = past_dates[-1]
                entry["end_date"] = rebalance_date
            window_idx = returns_idx.get_loc(rebalance_date)
            if window_idx >= self.window_days:
                first_valid_idx = i
                break
        if first_valid_idx is not None:
            rebalance_schedule = rebalance_schedule[first_valid_idx:]
        else:
            print("❌ Nicht genug Historie für dein Rolling Window!")
            print("Du brauchst Preisdaten ab:", self.user_start_date - pd.Timedelta(days=self.window_days+5))
            print("Erster möglicher Rebalance wäre frühestens:", returns.index[self.window_days])
            return


        # Erwartete Monate für späteres Reporting
        if len(rebalance_schedule) == 0:
            print(f"❌ Kein gültiger Rebalance-Termin im gesamten Zeitraum!")
            return

        first_rebalance_date = rebalance_schedule[0]['end_date']
        expected_months = pd.date_range(
            start=first_rebalance_date,
            end=self.price_data.index.max(),
            freq="ME"
        ).to_period("M")

        balance = self.start_balance
        self.current_cash = self.start_balance
        portfolio_values = pd.Series(index=self.price_data.index, dtype=float)
        self.selection_details = []
        current_positions = {}
        daily_data = []
        monthly_allocations = []
        self.total_trading_costs = 0.0

        last_prices = {}    # Speichert letzten Preis je Ticker für PnL

        for entry in rebalance_schedule:
            start_date = entry["start_date"]
            end_date = entry["end_date"]  # Das ist der Tag, an dem das Rebalancing stattfindet
            rebalance_date = end_date

            # 1) Performance TRACKING für ALLE Tage des Intervalls (mit aktuellen Positionen!)
            days = self.price_data.loc[start_date:end_date].index

            for day in days:
                # Berechne den Portfolio-Wert mit aktuellen Positionen für diesen Tag
                day_pnl_sum = 0
                for ticker, pos in current_positions.items():
                    price = self.price_data.at[day, ticker] if ticker in self.price_data.columns else np.nan
                    shares = pos["shares"]
                    weight = pos["weight"]
                    alloc_amount = shares * price

                    # PnL nur ab Tag 2
                    if ticker in last_prices:
                        pnl = (price - last_prices[ticker]) * shares
                    else:
                        pnl = 0
                    last_prices[ticker] = price
                    day_pnl_sum += pnl

                    daily_data.append({
                        "Date": day,
                        "Ticker": ticker,
                        "Close Price": price,
                        "Shares": shares,
                        "Allocated Amount": alloc_amount,
                        "Allocated Percentage (%)": weight * 100,
                        "PnL": pnl,
                        "Is_Rebalance_Day": day == rebalance_date,
                        "Trading Costs": pos.get("trading_costs", 0) if day == rebalance_date else 0,
                    })
                # Nur 1x pro Tag der Portfolio-Wert (inkl. Cash!)
                total_value = sum(pos["shares"] * self.price_data.at[day, t]
                                for t, pos in current_positions.items() if t in self.price_data.columns) + self.current_cash
                portfolio_values[day] = total_value

            # 2) AM LETZTEN TAG DES INTERVALLS: REBALANCING!
            # Nur am rebalance_date (=end_date) erfolgt die Umschichtung
            # (danach laufen die neuen Positionen ab dem nächsten Intervall)
            if rebalance_date in self.price_data.index:
                # === Overlay (Aktienquote) bestimmen wie gehabt ===
                overlay_data = pd.DataFrame({
                    "close": self.price_data.loc[(rebalance_date - pd.Timedelta(days=self.window_days)):rebalance_date].mean(axis=1),
                    "sentiment": np.random.normal(0, 1, size=len(self.price_data.loc[(rebalance_date - pd.Timedelta(days=self.window_days)):rebalance_date]))
                })
                
                # Rolling Window: Immer die letzten self.window_days HANDELSTAGE bis inklusive rebalance_date!
                if rebalance_date not in returns.index:
                    continue  # Kein Preis für diesen Tag

                window_idx = returns.index.get_loc(rebalance_date)
                if window_idx < self.window_days - 1:
                    continue  # Nicht genug Historie

                # Rolling Window holen
                sub_returns = returns.iloc[window_idx - self.window_days + 1 : window_idx + 1]

                # Ticker mit mindestens 95% Daten im Window
                min_valid_days = int(self.window_days * 0.95)  # oder 0.95, je nach Wunsch!
                valid_tickers = [col for col in sub_returns.columns if sub_returns[col].count() >= min_valid_days]

                if len(valid_tickers) == 0:
                    # KEIN einziger Ticker mit ausreichend Daten → Monat überspringen
                    continue

                # Nimm ALLE verfügbaren Ticker, egal wie viele das sind (solange >0)
                sub_returns = sub_returns[valid_tickers].fillna(0)
                n_stocks = min(self.num_stocks, len(valid_tickers))




                available = sub_returns.shape[1]

                if self.risk_overlay is not None:
                    agg_scores = self.risk_overlay.aggregate_scores(overlay_data)
                    ziel_aktienquote = self.risk_overlay.map_to_equity_weight(agg_scores)
                else:
                    ziel_aktienquote = 1.0

                if ziel_aktienquote == 0:
                    liquidation_value = 0
                    for ticker, pos in current_positions.items():
                        if ticker in self.price_data.columns:
                            price = self.price_data.at[rebalance_date, ticker]
                            if not np.isnan(price):
                                liquidation_value += pos["shares"] * price
                    self.current_cash += liquidation_value
                    current_positions = {}
                    # ... Log etc.
                else:
                    if available == 0:
                        continue
                    n_stocks = min(self.num_stocks, available)
                    top_universe = select_top_sharpe_tickers(sub_returns, top_universe_size)
                    filtered_returns = sub_returns[top_universe]

                    if self.optimization_mode == "select-then-optimize":
                        top_tickers = filtered_returns.columns[:n_stocks]
                        filtered_top = filtered_returns[top_tickers]
                        weights_series = optimize_portfolio(
                            returns=filtered_top,
                            method=self.optimizer_method,
                            cov_estimator=self.cov_estimator,
                            min_weight=self.min_weight,
                            max_weight=self.max_weight,
                            force_equal_weight=self.force_equal_weight,
                            debug_label="A - Optimizer only weight",
                            num_stocks=n_stocks,
                        )
                    else:
                        weights_full = optimize_portfolio(
                            returns=filtered_returns,
                            method=self.optimizer_method,
                            cov_estimator=self.cov_estimator,
                            min_weight=self.min_weight,
                            max_weight=self.max_weight,
                            force_equal_weight=self.force_equal_weight,
                            debug_label="B - Optimizer selects & weights",
                            num_stocks=n_stocks,
                        )
                        top_tickers = weights_full.sort_values(ascending=False).head(n_stocks).index.tolist()
                        weights_series = weights_full.loc[top_tickers]

                    total_portfolio_value = sum(
                        pos["shares"] * self.price_data.at[rebalance_date, t]
                        for t, pos in current_positions.items()
                        if t in self.price_data.columns and not np.isnan(self.price_data.at[rebalance_date, t])
                    ) + self.current_cash

                    # 2) Ziel­allokation berechnen
                    target_equity_value = total_portfolio_value * ziel_aktienquote
                    weights = weights_series.values           
                    investierbarer_betrag = target_equity_value

                    current_positions, new_allocs = allocate_positions(
                        self.price_data,
                        top_tickers,
                        weights,
                        rebalance_date,
                        investierbarer_betrag,
                        previous_positions=current_positions,
                        enable_trading_costs=self.enable_trading_costs,
                        fixed_cost_per_trade=self.fixed_cost_per_trade,
                        variable_cost_pct=self.variable_cost_pct,
                    )
                    
                    invested = sum(alloc.get("Value", 0)            # korrektes Feld
                    for alloc in new_allocs
                    if alloc.get("Ticker") != "TOTAL_COSTS")

                    costs    = next(                                   # nur die Summary-Zeile
                                (alloc["Trading Costs"] for alloc in new_allocs
                                 if alloc.get("Ticker") == "TOTAL_COSTS"),
                                0.0)
                    self.current_cash = total_portfolio_value - invested - costs
                    self.total_trading_costs += costs
                    self.selection_details.append({
                        "Rebalance Date": rebalance_date,
                        "Actual Rebalance Day": rebalance_date,
                        "Top Universe Size": len(valid_tickers),
                        "Optimization Method": self.optimizer_method,
                        "Cov Estimator": self.cov_estimator,
                        "Selected Tickers": valid_tickers,
                        "Rebalance Frequency": self.rebalance_freq,
                        "Total Trading Costs": self.total_trading_costs,
                        "Trading Costs %": (self.total_trading_costs/self.start_balance)*100,
                    })
                    monthly_allocations.extend(new_allocs)

        # Ergebnis-DataFrames füllen
        self.portfolio_value     = portfolio_values.dropna()
        self.true_daily_portfolio_pnl = self.portfolio_value.diff().fillna(0)
        self.daily_df            = pd.DataFrame(daily_data)
        self.monthly_allocations = pd.DataFrame(monthly_allocations)

        # SUMMARY-Zeile für Trading-Kosten
        self.selection_details.append({
            "Rebalance Date":      "SUMMARY",
            "Actual Rebalance Day":"SUMMARY",
            "Top Universe Size":   0,
            "Optimization Method": "N/A",
            "Cov Estimator":       "N/A",
            "Selected Tickers":    "N/A",
            "Rebalance Frequency": self.rebalance_freq,
            "Total Trading Costs": self.total_trading_costs,
            "Trading Costs %":     (self.total_trading_costs/self.start_balance)*100,
        })

        # Fehlende Monate protokollieren
        actual_months = (
            pd.to_datetime([d["Rebalance Date"] for d in self.selection_details if d["Rebalance Date"]!="SUMMARY"])
            .to_series().dt.to_period("M").drop_duplicates()
        )
        self.missing_months = [
            m.strftime("%Y-%m")
            for m in expected_months
            if m not in actual_months.values
        ]
        if self.missing_months:
            log = "⚠️ Rebalance fehlt für folgende Monate: " + ", ".join(self.missing_months)
            print(log)
            self.log_lines.append(log)

        # — Next-Month Allocation analog berechnen (kürzere Logik) —
        last_date = self.price_data.index.max()
        idx = returns.index
        if last_date not in returns.index:
            sub_returns = pd.DataFrame()  # Defensive
        else:
            window_idx = returns.index.get_loc(last_date)
            if window_idx < self.window_days:
                sub_returns = pd.DataFrame()
            else:
                sub_returns = returns.iloc[window_idx - self.window_days + 1 : window_idx + 1]



        available_nm = sub_returns.shape[1]
        if available_nm == 0:
            self.next_month_tickers = []
            self.next_month_weights = pd.Series(dtype=float)
        else:
            n_nm = min(self.num_stocks, available_nm)
            if self.optimization_mode == "select-then-optimize":
                top_sharpe = select_top_sharpe_tickers(sub_returns, top_universe_size)[:n_nm]
                weights_nm = optimize_portfolio(
                    returns=sub_returns[top_sharpe],
                    method=self.optimizer_method,
                    cov_estimator=self.cov_estimator,
                    min_weight=self.min_weight,
                    max_weight=self.max_weight,
                    force_equal_weight=self.force_equal_weight,
                    debug_label="NextMonth A",
                    num_stocks=n_nm,
                )
            else:
                wf = optimize_portfolio(
                    returns=sub_returns,
                    method=self.optimizer_method,
                    cov_estimator=self.cov_estimator,
                    min_weight=self.min_weight,
                    max_weight=self.max_weight,
                    force_equal_weight=self.force_equal_weight,
                    debug_label="NextMonth B",
                    num_stocks=n_nm,
                )
                top_sharpe = wf.sort_values(ascending=False).head(n_nm).index.tolist()
                weights_nm = wf.loc[top_sharpe]

            # im statischen Modus auf originale tickers einschränken
            if self.universe_mode == "static":
                top_sharpe = [t for t in top_sharpe if t in self.price_data.columns]
                weights_nm = weights_nm.reindex(top_sharpe).fillna(0)

            self.next_month_tickers = top_sharpe
            self.next_month_weights = weights_nm

        print("🔮 Next-Month-Universe:", getattr(self, "next_month_tickers", []))
        
        self._calculate_performance_metrics()

        # Ergebnisse auf Backtest-Start croppen
        self.portfolio_value = self.portfolio_value.loc[self.user_start_date:]
        self.daily_df = self.daily_df[self.daily_df["Date"] >= self.user_start_date]
        self.true_daily_portfolio_pnl = self.true_daily_portfolio_pnl.loc[self.user_start_date:]
        


        return self.portfolio_value
        
    def _calculate_performance_metrics(self):
        """
        Berechnet alle Performance‑Kennzahlen des Backtests und legt sie in
        `self.performance_metrics` sowie `self.monthly_performance` ab.
        KORRIGIERT: Monatliche PnL-Berechnung berücksichtigt jetzt Rebalancing-Effekte korrekt.
        """

        # ------------------------------------------------------------
        # 0) Grundvoraussetzung
        # ------------------------------------------------------------
        if self.portfolio_value.empty:
            self.performance_metrics = pd.DataFrame()
            self.monthly_performance = pd.DataFrame()
            return

        # ------------------------------------------------------------
        # 1) Basisgrößen
        # ------------------------------------------------------------
        daily_returns = self.portfolio_value.pct_change().dropna()

        total_return  = self.portfolio_value.iloc[-1] / self.start_balance - 1
        cagr          = (self.portfolio_value.iloc[-1] / self.start_balance) ** (
            252 / len(self.portfolio_value)
        ) - 1
        volatility    = daily_returns.std() * np.sqrt(252)
        sharpe        = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)

        rolling_max   = self.portfolio_value.cummax()
        drawdown      = (self.portfolio_value / rolling_max) - 1
        max_dd        = drawdown.min()

        trading_costs_pct = self.total_trading_costs / self.start_balance * 100

        # ------------------------------------------------------------
        # 2) Zusätzliche Risiko‑Kennzahlen
        # ------------------------------------------------------------
        ui  = np.sqrt(((drawdown[drawdown < 0] * 100) ** 2).mean())
        upi = cagr / (ui / 100) if ui != 0 else np.nan

        downside = daily_returns[daily_returns < 0]
        down_vol = downside.std() * np.sqrt(252)
        rf_daily = 0.02 / 252
        sortino  = ((daily_returns.mean() - rf_daily) / down_vol) * np.sqrt(252) if down_vol != 0 else np.nan

        calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan

        theta  = 0.0
        pos    = (daily_returns - theta).clip(lower=0).sum()
        neg    = (theta - daily_returns).clip(lower=0).sum()
        omega  = pos / neg if neg != 0 else np.nan

        avg_dd = abs(drawdown[drawdown < 0]).mean()
        pain   = cagr / avg_dd if avg_dd != 0 else np.nan

        # ------------------------------------------------------------
        # 3) DataFrame zusammenstellen
        # ------------------------------------------------------------
        base_metrics_df = pd.DataFrame(
            {
                "Metric": [
                    "Start Balance",
                    "End Balance",
                    "Total Return (%)",
                    "CAGR (%)",
                    "Annual Volatility (%)",
                    "Sharpe Ratio",
                    "Max Drawdown (%)",
                    "Total Trading Costs",
                    "Trading Costs (% of Initial)",
                ],
                "Value": [
                    f"${self.start_balance:,.2f}",
                    f"${self.portfolio_value.iloc[-1]:,.2f}",
                    f"{total_return * 100:.2f}%",
                    f"{cagr * 100:.2f}%",
                    f"{volatility * 100:.2f}%",
                    f"{sharpe:.2f}",
                    f"{max_dd * 100:.2f}%",
                    f"${self.total_trading_costs:,.2f}",
                    f"{trading_costs_pct:.2f}%",
                ],
            }
        )

        extra_metrics_df = pd.DataFrame(
            {
                "Metric": [
                    "Ulcer Index",
                    "Ulcer Performance Index",
                    "Sortino Ratio",
                    "Calmar Ratio",
                    "Omega Ratio",
                    "Pain Ratio",
                ],
                "Value": [
                    f"{ui:.2f}",
                    f"{upi:.2f}",
                    f"{sortino:.2f}",
                    f"{calmar:.2f}",
                    f"{omega:.2f}",
                    f"{pain:.2f}",
                ],
            }
        )

        self.performance_metrics = pd.concat(
            [base_metrics_df, extra_metrics_df], ignore_index=True
        )

        # ------------------------------------------------------------
        # 4) KORRIGIERTE PnL-Berechnung
        # ------------------------------------------------------------

        # --- True daily PnL aus Portfolio-Value (unverändert)
        self.true_daily_portfolio_pnl = self.portfolio_value.diff().fillna(0)
        self.true_daily_portfolio_pnl.index = pd.to_datetime(self.true_daily_portfolio_pnl.index)

        # --- NEU: Marktbasierte tägliche Returns berechnen (ohne Rebalancing-Effekte)
        market_daily_pnl = self._calculate_market_based_daily_pnl()

        # --- Summe der Einzel-Tages-PnLs (unverändert für Vergleich)
        if not np.issubdtype(self.daily_df["Date"].dtype, np.datetime64):
            self.daily_df["Date"] = pd.to_datetime(self.daily_df["Date"])
        sum_daily_trade_pnl = self.daily_df.groupby("Date")["PnL"].sum().reindex(self.true_daily_portfolio_pnl.index).fillna(0)

        # --- Tagesvergleich DataFrame (erweitert)
        daily_compare = pd.DataFrame({
            "Portfolio-Value-diff": self.true_daily_portfolio_pnl,
            "Market-based-PnL": market_daily_pnl,
            "Summe Einzel-Tages-PnL": sum_daily_trade_pnl,
            "Abweichung (Portfolio vs Market)": self.true_daily_portfolio_pnl - market_daily_pnl,
            "Abweichung (Portfolio vs Summe)": self.true_daily_portfolio_pnl - sum_daily_trade_pnl
        })
        
        print("\n=== DEBUG: Erweiterte tägliche PnL-Vergleiche ===")
        print(daily_compare.head(30))

        # --- KORRIGIERTE Monats-PnL: Verwende marktbasierte Returns
        monthly_market_pnl = market_daily_pnl.resample("ME").sum()
        monthly_portfolio_diff = self.true_daily_portfolio_pnl.resample("ME").sum()
        
        # --- Jahres-PnL
        yearly_market_pnl = market_daily_pnl.resample("YE").sum()
        yearly_portfolio_diff = self.true_daily_portfolio_pnl.resample("YE").sum()

        if "PnL" in self.daily_df.columns:
            daily_pnl_per_day = self.daily_df.groupby("Date")["PnL"].sum()
            daily_pnl_per_day.index = pd.to_datetime(daily_pnl_per_day.index)
            monthly_pnl_sum = daily_pnl_per_day.resample("ME").sum()
            yearly_pnl_sum = daily_pnl_per_day.resample("YE").sum()
        else:
            monthly_pnl_sum = pd.Series(dtype=float)
            yearly_pnl_sum = pd.Series(dtype=float)

        # --- KORRIGIERTE Vergleichs-DataFrames
        monthly_compare = pd.DataFrame({
            "Monthly PnL (Portfolio Value Diff)": monthly_portfolio_diff,
            "Monthly PnL (Market-based)": monthly_market_pnl,
            "Monthly PnL (Summe Tages-PnL)": monthly_pnl_sum,
            "Rebalancing Impact": monthly_portfolio_diff - monthly_market_pnl
        })

        yearly_compare = pd.DataFrame({
            "Yearly PnL (Portfolio Value Diff)": yearly_portfolio_diff,
            "Yearly PnL (Market-based)": yearly_market_pnl,
            "Yearly PnL (Summe Tages-PnL)": yearly_pnl_sum,
            "Rebalancing Impact": yearly_portfolio_diff - yearly_market_pnl
        })

        # --- Cropping
        monthly_compare = monthly_compare[
            (monthly_compare.index >= self.user_start_date) &
            (monthly_compare["Monthly PnL (Portfolio Value Diff)"].notna())
        ]
        yearly_compare = yearly_compare[
            (yearly_compare.index >= self.user_start_date) &
            (yearly_compare["Yearly PnL (Portfolio Value Diff)"].notna())
        ]
        
        self.monthly_compare = monthly_compare
        self.yearly_compare = yearly_compare

        print("\n=== KORRIGIERTE monatliche PnL-Analyse ===")
        print("Monthly Compare (erste 12 Monate):")
        print(monthly_compare.head(12))

        # --- Export
        try:
            daily_compare.to_csv("daily_pnl_compare_extended.csv")
            monthly_compare.to_csv("monthly_compare_corrected.csv", index=True)
            yearly_compare.to_csv("yearly_compare_corrected.csv", index=True)
            with pd.ExcelWriter("pnl_comparisons_corrected.xlsx") as writer:
                daily_compare.to_excel(writer, sheet_name="Daily")
                monthly_compare.to_excel(writer, sheet_name="Monthly")
                yearly_compare.to_excel(writer, sheet_name="Yearly")
            print("✅ KORRIGIERTE Vergleichs-DataFrames erfolgreich exportiert!")
        except Exception as e:
            print(f"❌ Fehler beim Export: {e}")

        # --- Monats-Performance DataFrame für UI (verwende marktbasierte PnL)
        pv_cropped = self.portfolio_value.copy()
        user_start = pd.to_datetime(self.user_start_date)
        prev_month_end = (user_start - pd.offsets.MonthEnd(1)).normalize()

        if prev_month_end not in pv_cropped.index:
            pv_cropped.loc[prev_month_end] = self.start_balance
            pv_cropped = pv_cropped.sort_index()

        pv_cropped = pv_cropped[pv_cropped.index >= prev_month_end]

        # Verwende marktbasierte monatliche PnL für korrekte Darstellung
        monthly_market_pnl_cropped = monthly_market_pnl[monthly_market_pnl.index >= self.user_start_date]
        
        if not monthly_market_pnl_cropped.empty:
            # Berechne monatliche Returns basierend auf marktbasierten PnL
            month_end_values = pv_cropped.groupby([pv_cropped.index.year, pv_cropped.index.month]).tail(1)
            monthly_returns = monthly_market_pnl_cropped / month_end_values.shift(1).reindex(monthly_market_pnl_cropped.index).fillna(self.start_balance) * 100
            
            common_index = monthly_market_pnl_cropped.index.intersection(monthly_returns.index)
            if len(common_index) > 0:
                self.monthly_performance = pd.DataFrame({
                    "Date": monthly_market_pnl_cropped.loc[common_index].index,
                    "Monthly PnL ($)": monthly_market_pnl_cropped.loc[common_index].values,
                    "Monthly PnL (%)": monthly_returns.loc[common_index].values,
                })
            else:
                self.monthly_performance = pd.DataFrame()
        else:
            self.monthly_performance = pd.DataFrame()

    def _calculate_market_based_daily_pnl(self):
        """
        Berechnet tägliche PnL basierend nur auf Marktbewegungen, 
        ohne Rebalancing-Effekte zu berücksichtigen.
        """
        if self.daily_df.empty:
            return pd.Series(dtype=float, index=self.portfolio_value.index)
        
        # Stelle sicher, dass Date als datetime vorliegt
        if not np.issubdtype(self.daily_df["Date"].dtype, np.datetime64):
            self.daily_df["Date"] = pd.to_datetime(self.daily_df["Date"])
        
        market_pnl_series = pd.Series(dtype=float, index=self.portfolio_value.index)
        
        # Gruppiere nach Datum und berechne marktbasierte PnL
        for date, group in self.daily_df.groupby("Date"):
            # Nur echte Markt-PnL (ohne Rebalancing-Tage)
            if not group["Is_Rebalance_Day"].any():
                # Normaler Handelstag: Summe der PnL aller Positionen
                daily_market_pnl = group["PnL"].sum()
            else:
                # Rebalancing-Tag: Berechne PnL nur basierend auf Marktbewegung vor Rebalancing
                # Verwende die Marktbewegung, aber ignoriere Rebalancing-Effekte
                non_rebalance_pnl = 0
                for _, row in group.iterrows():
                    if row["Shares"] > 0 and not pd.isna(row["Close Price"]):
                        # Für Rebalancing-Tage: Schätze die Marktbewegung
                        # ohne den Rebalancing-Effekt
                        if date in self.portfolio_value.index:
                            prev_date_idx = self.portfolio_value.index.get_loc(date) - 1
                            if prev_date_idx >= 0:
                                prev_date = self.portfolio_value.index[prev_date_idx]
                                if row["Ticker"] in self.price_data.columns:
                                    prev_price = self.price_data.at[prev_date, row["Ticker"]]
                                    curr_price = row["Close Price"]
                                    if not pd.isna(prev_price) and not pd.isna(curr_price):
                                        # Marktbewegung ohne Rebalancing-Effekt
                                        price_change = (curr_price - prev_price) / prev_price
                                        # Verwende die Shares vom Vortag (vor Rebalancing)
                                        prev_value = row["Shares"] * prev_price
                                        market_pnl_only = prev_value * price_change
                                        non_rebalance_pnl += market_pnl_only
                daily_market_pnl = non_rebalance_pnl
            
            market_pnl_series.loc[date] = daily_market_pnl
        
        return market_pnl_series.fillna(0)