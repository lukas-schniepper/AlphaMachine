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
    MAX_TURNOVER,
    MAX_SECTOR_WEIGHT,
    MIN_CAGR,
    USE_BALANCED_OBJECTIVE,
    USE_BENCHMARK,
    BENCHMARK_TICKERS,
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
        price_data,
        start_balance,
        num_stocks,
        optimize_weights=None,
        optimizer_method=None,
        cov_estimator=None,
        min_weight=MIN_WEIGHT,
        max_weight=MAX_WEIGHT,
        window_days=BACKTEST_WINDOW_DAYS,
        force_equal_weight=FORCE_EQUAL_WEIGHT,
        rebalance_frequency=REBALANCE_FREQUENCY,
        custom_rebalance_months=CUSTOM_REBALANCE_MONTHS,
        enable_trading_costs=ENABLE_TRADING_COSTS,
        fixed_cost_per_trade=FIXED_COST_PER_TRADE,
        variable_cost_pct=VARIABLE_COST_PCT,
    ):

        self.optimizer_method = (
            optimizer_method if optimizer_method is not None else OPTIMIZER_METHOD
        )
        self.price_data = price_data
        self.start_balance = start_balance
        self.num_stocks = num_stocks
        self.optimize_weights = (
            optimize_weights if optimize_weights is not None else OPTIMIZE_WEIGHTS
        )
        self.cov_estimator = (
            cov_estimator if cov_estimator is not None else COV_ESTIMATOR
        )
        self.min_weight = min_weight
        self.max_weight = max_weight
        self.window_days = window_days
        self.portfolio_value = pd.Series(dtype=float)
        self.daily_df = pd.DataFrame()
        self.selection_details = []
        self.monthly_allocations = pd.DataFrame()
        self.filtered_tickers = []
        self.filtered_tickers_by_month = {}
        self.monthly_filtered_report = []
        self.log_lines = []
        self.ticker_coverage_logs = []
        self.missing_months = []
        self.performance_metrics = pd.DataFrame()
        self.monthly_performance = pd.DataFrame()
        self._filter_complete_tickers()
        self.rebalance_freq = rebalance_frequency  # "weekly", "monthly" oder custom
        self.custom_rebalance_months = custom_rebalance_months
        self.max_turnover = MAX_TURNOVER
        self.use_benchmark = USE_BENCHMARK
        self.optimization_mode = OPTIMIZATION_MODE
        self.force_equal_weight = force_equal_weight
        self.enable_trading_costs = enable_trading_costs
        self.fixed_cost_per_trade = fixed_cost_per_trade
        self.variable_cost_pct = variable_cost_pct
        self.total_trading_costs = 0.0

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

    def run_with_next_month_allocation(
        self,
        top_universe_size=100,
        optimizer_method="ledoit-wolf",
        force_equal_weight=False,
    ):
        returns = self.price_data.pct_change().dropna()
        rebalance_schedule = build_rebalance_schedule(
            self.price_data,
            frequency=self.rebalance_freq,
            custom_months=self.custom_rebalance_months,
        )
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
            sub_returns = returns.loc[window_start:date].dropna(axis=1, how="all")

            if sub_returns.shape[1] < self.num_stocks:
                print(
                    f"⚠️ {date.date()}: Not enough tickers ({sub_returns.shape[1]}). Skipping rebalance."
                )
                continue

            top_universe = select_top_sharpe_tickers(sub_returns, top_universe_size)
            filtered_returns = sub_returns[top_universe]

            print(
                f"🔍 Rebalance {date.date()} | Optimizer: {optimizer_method} | Mode: {self.optimization_mode} | CovEstimator: {self.cov_estimator}"
            )

            if self.optimization_mode == "select-then-optimize":
                top_tickers = filtered_returns.columns[: self.num_stocks]
                filtered_top_returns = filtered_returns[top_tickers]

                weights_series = optimize_portfolio(
                    returns=filtered_top_returns,
                    method=self.optimizer_method,
                    cov_estimator=self.cov_estimator,
                    min_weight=self.min_weight,
                    max_weight=self.max_weight,
                    force_equal_weight=self.force_equal_weight,
                    debug_label="A - Optimizer only weight",
                    num_stocks=self.num_stocks,
                )

            elif self.optimization_mode == "optimize-subset":
                weights_series = optimize_portfolio(
                    returns=filtered_returns,
                    method=self.optimizer_method,
                    cov_estimator=self.cov_estimator,
                    min_weight=self.min_weight,
                    max_weight=self.max_weight,
                    force_equal_weight=self.force_equal_weight,
                    debug_label="B - Optimizer selects & weights",
                    num_stocks=self.num_stocks,
                )
                top_tickers = weights_series.index.tolist()

            else:
                raise ValueError(
                    f"❌ Unbekannter Optimierungsmodus: {self.optimization_mode}"
                )

            selection_detail = {
                "Rebalance Date": date.date(),
                "Actual Rebalance Day": date.strftime("%Y-%m-%d"),
                "Top Universe Size": len(top_universe),
                "Optimization Method": optimizer_method,
                "Cov Estimator": self.cov_estimator,
                "Selected Tickers": ", ".join(top_tickers),
                "Rebalance Frequency": self.rebalance_freq,
            }
            self.selection_details.append(selection_detail)

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

            # Trading-Kosten aus Allokationen extrahieren
            rebalance_costs = 0
            for alloc in new_allocs:
                if "Trading Costs" in alloc:
                    rebalance_costs += alloc["Trading Costs"]

            self.total_trading_costs += rebalance_costs

            monthly_allocations.extend(new_allocs)

            log_msg = f"🔁 {date.date()}: Rebalanced for {start_date.date()} - {end_date.date()} | {len(current_positions)} positions | Trading Costs: {rebalance_costs:.2f}"
            print(log_msg)
            self.log_lines.append(log_msg)

            for day in self.price_data.loc[start_date:end_date].index:
                daily_value = 0
                for ticker, pos in current_positions.items():
                    if ticker in self.price_data.columns:
                        price = self.price_data.loc[day, ticker]
                        if not np.isnan(price):
                            value = pos["shares"] * price
                            daily_value += value
                            daily_data.append(
                                {
                                    "Date": day.date(),
                                    "Ticker": ticker,
                                    "Close Price": price,
                                    "Shares": pos["shares"],
                                    "Allocated Amount": value,
                                    "Allocated Percentage (%)": pos["weight"] * 100,
                                    "Total Portfolio Value": daily_value,
                                    "Is_Rebalance_Day": day == start_date,
                                    "Trading Costs": (
                                        pos.get("trading_costs", 0)
                                        if day == start_date
                                        else 0
                                    ),
                                }
                            )
                portfolio_values[day] = daily_value
                balance = daily_value

        self.portfolio_value = portfolio_values.dropna()
        self.daily_df = pd.DataFrame(daily_data)
        self.monthly_allocations = pd.DataFrame(monthly_allocations)

        # Füge Trading-Kosten zur Performance-Zusammenfassung hinzu
        selection_detail = {
            "Rebalance Date": "SUMMARY",
            "Actual Rebalance Day": "SUMMARY",
            "Top Universe Size": 0,
            "Optimization Method": "N/A",
            "Cov Estimator": "N/A",
            "Selected Tickers": "N/A",
            "Rebalance Frequency": self.rebalance_freq,
            "Total Trading Costs": self.total_trading_costs,
            "Trading Costs %": (self.total_trading_costs / self.start_balance) * 100,
        }
        self.selection_details.append(selection_detail)

        actual_months = (
            pd.to_datetime(
                [
                    d["Rebalance Date"]
                    for d in self.selection_details
                    if d["Rebalance Date"] != "SUMMARY"
                ]
            )
            .to_series()
            .dt.to_period("M")
            .drop_duplicates()
        )

        self.missing_months = [
            m.strftime("%Y-%m")
            for m in expected_months
            if m not in actual_months.values
        ]

        if self.missing_months:
            log = "⚠️ Rebalance fehlt für folgende Monate: " + ", ".join(
                self.missing_months
            )
            print(log)
            self.log_lines.append(log)

        self._calculate_performance_metrics()
        return self.portfolio_value

    def _calculate_performance_metrics(self):
        if self.portfolio_value.empty:
            self.performance_metrics = pd.DataFrame()
            return

        daily_returns = self.portfolio_value.pct_change().dropna()
        total_return = self.portfolio_value.iloc[-1] / self.start_balance - 1
        cagr = (self.portfolio_value.iloc[-1] / self.start_balance) ** (
            252 / len(self.portfolio_value)
        ) - 1
        volatility = daily_returns.std() * np.sqrt(252)
        sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
        rolling_max = self.portfolio_value.cummax()
        drawdown = (self.portfolio_value / rolling_max) - 1
        max_dd = drawdown.min()

        # Trading-Kosten als Prozentsatz des ursprünglichen Kapitals
        trading_costs_pct = self.total_trading_costs / self.start_balance * 100

        self.performance_metrics = pd.DataFrame(
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

        monthly_pnl = self.portfolio_value.resample("ME").last().pct_change().dropna()
        monthly_abs = self.portfolio_value.resample("ME").last().diff().dropna()
        self.monthly_performance = pd.DataFrame(
            {
                "Date": monthly_pnl.index.date,
                "Monthly PnL ($)": monthly_abs.values,
                "Monthly PnL (%)": monthly_pnl.values * 100,
            }
        )
