import pandas as pd
import numpy as np
from AlphaMachine_core.optimizers import optimize_portfolio
from AlphaMachine_core.utils import (
    build_rebalance_schedule,
    select_top_sharpe_tickers,
    allocate_positions,
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
        # ➊ Unveränderliche Kern-Parameter
        self.price_data      = price_data
        self.start_balance   = start_balance
        self.num_stocks      = num_stocks
        self.start_month     = pd.Period(start_month, "M")

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


    def _get_valid_tickers(self, threshold=0.95):
        full_range = pd.date_range(
            start=self.price_data.index.min(), end=self.price_data.index.max(), freq="B"
        )
        min_coverage_days = int(len(full_range) * threshold)
        valid = []
        invalid = []
        missing_by_month = {}

        for col in self.price_data.columns:
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

        # DEBUG
        print("🗓️ Rebalance Schedule:",
            [e["rebalance_date"].strftime("%Y-%m-%d") for e in rebalance_schedule])
        print("▶️ schedule Länge:", len(rebalance_schedule))
        print("⚙️ custom_rebalance_months =", self.custom_rebalance_months,
            "| rebalance_freq =", self.rebalance_freq)

        # Erwartete Monate für späteres Reporting
        expected_months = pd.date_range(
            start=self.price_data.index.min(),
            end=self.price_data.index.max(),
            freq="ME",
        ).to_period("M")

        balance = self.start_balance
        portfolio_values = pd.Series(index=self.price_data.index, dtype=float)
        self.selection_details = []
        current_positions = {}
        daily_data = []
        monthly_allocations = []
        self.total_trading_costs = 0.0

        for entry in rebalance_schedule:
            date = entry["rebalance_date"]
            start_date = entry["start_date"]
            end_date = entry["end_date"]
            window_start = date - pd.Timedelta(days=self.window_days)

            # Renditen im Lookback-Fenster, entferne Ticker ohne Daten in diesem Fenster
            sub_returns = returns.loc[window_start:date].dropna(axis=1, how="all")
            available = sub_returns.shape[1]

            # Wenn gar keine Ticker da sind, überspringen
            if available == 0:
                print(f"⚠️ {date.date()}: Kein einziger Ticker verfügbar. Skipping rebalance.")
                continue

            # Auf verfügbare Anzahl runterrechnen
            n_stocks = min(self.num_stocks, available)
            if available < self.num_stocks:
                print(f"⚠️ {date.date()}: Nur {available} Ticker verfügbar, verwende {n_stocks}.")
                self.log_lines.append(
                    f"⚠️ {date.date()}: Only {available} tickers available, using {n_stocks} instead of {self.num_stocks}"
                )

            # Pre-Selection nach Sharpe
            top_universe = select_top_sharpe_tickers(sub_returns, top_universe_size)
            filtered_returns = sub_returns[top_universe]

            print(
                f"🔍 Rebalance {date.date()}"
                f" | Optimizer: {self.optimizer_method}"
                f" | Mode: {self.optimization_mode}"
                f" | CovEstimator: {self.cov_estimator}"
            )

            # Gewicht-Optimierung oder Subset-Optimierung
            if self.optimization_mode == "select-then-optimize":
                # Variante A: zuerst Top N, dann Gewichte optimieren
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
                # Variante B: gesamte Menge gewichten, dann Top N nach Gewicht auswählen
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

            # Log-Detail
            self.selection_details.append({
                "Rebalance Date":        date.strftime("%Y-%m-%d"),
                "Actual Rebalance Day":  date.strftime("%Y-%m-%d"),
                "Top Universe Size":     len(top_universe),
                "Optimization Method":   self.optimizer_method,
                "Cov Estimator":         self.cov_estimator,
                "Selected Tickers":      ", ".join(top_tickers),
                "Rebalance Frequency":   self.rebalance_freq,
            })

            # Positions anlegen
            weights = weights_series.values
            current_positions, new_allocs = allocate_positions(
                self.price_data,
                top_tickers,
                weights,
                date,
                balance,
                previous_positions=current_positions,
                enable_trading_costs=self.enable_trading_costs,
                fixed_cost_per_trade=self.fixed_cost_per_trade,
                variable_cost_pct=self.variable_cost_pct,
            )

            # Trading-Kosten summieren
            rebalance_costs = sum(alloc.get("Trading Costs", 0) for alloc in new_allocs)
            self.total_trading_costs += rebalance_costs
            monthly_allocations.extend(new_allocs)

            log_msg = (f"🔁 {date.date()}: Rebalanced for "
                    f"{start_date.date()} - {end_date.date()} | "
                    f"{len(current_positions)} positions | "
                    f"Trading Costs: {rebalance_costs:.2f}")
            print(log_msg)
            self.log_lines.append(log_msg)

            # Daily-Portfolio-Werte
            for day in self.price_data.loc[start_date:end_date].index:
                daily_value = 0
                for ticker, pos in current_positions.items():
                    price = self.price_data.at[day, ticker] if ticker in self.price_data.columns else np.nan
                    if not np.isnan(price):
                        value = pos["shares"] * price
                        daily_value += value
                        daily_data.append({
                            "Date":                 day.date(),
                            "Ticker":               ticker,
                            "Close Price":          price,
                            "Shares":               pos["shares"],
                            "Allocated Amount":     value,
                            "Allocated Percentage (%)": pos["weight"]*100,
                            "Total Portfolio Value":    daily_value,
                            "Is_Rebalance_Day":         day == start_date,
                            "Trading Costs":            pos.get("trading_costs",0) if day==start_date else 0,
                        })
                portfolio_values[day] = daily_value
                balance = daily_value

        # Ergebnis-DataFrames füllen
        self.portfolio_value     = portfolio_values.dropna()
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
        last_date    = self.price_data.index.max()
        window_start = last_date - pd.Timedelta(days=self.window_days)
        sub_returns  = returns.loc[window_start:last_date].dropna(axis=1, how="all")
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

        return self.portfolio_value

    def _calculate_performance_metrics(self):
        """
        Berechnet alle Performance‑Kennzahlen des Backtests und legt sie in
        `self.performance_metrics` sowie `self.monthly_performance` ab.
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
        # 2‑A) Ulcer Index & UPI
        ui  = np.sqrt(((drawdown[drawdown < 0] * 100) ** 2).mean())                # Prozent‑Skala
        upi = cagr / (ui / 100) if ui != 0 else np.nan

        # 2‑B) Sortino Ratio (Downside‑Vol)
        downside = daily_returns[daily_returns < 0]
        down_vol = downside.std() * np.sqrt(252)
        rf_daily = 0.02 / 252                                                     # 2 % p. a. Beispiel
        sortino  = ((daily_returns.mean() - rf_daily) / down_vol) * np.sqrt(252) if down_vol != 0 else np.nan

        # 2‑C) Calmar Ratio
        calmar = cagr / abs(max_dd) if max_dd != 0 else np.nan

        # 2‑D) Omega Ratio (θ = 0 %)
        theta  = 0.0
        pos    = (daily_returns - theta).clip(lower=0).sum()
        neg    = (theta - daily_returns).clip(lower=0).sum()
        omega  = pos / neg if neg != 0 else np.nan

        # 2‑E) Pain Ratio
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
        # 4) Monatliche Performance‑Tabelle
        # ------------------------------------------------------------
        monthly_pnl = self.portfolio_value.resample("ME").last().pct_change().dropna()
        monthly_abs = self.portfolio_value.resample("ME").last().diff().dropna()

        self.monthly_performance = pd.DataFrame(
            {
                "Date":            monthly_pnl.index.date,
                "Monthly PnL ($)": monthly_abs.values,
                "Monthly PnL (%)": monthly_pnl.values * 100,
            }
        )

