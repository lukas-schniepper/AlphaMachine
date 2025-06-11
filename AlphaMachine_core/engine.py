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
        use_risk_overlay: bool = True,
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
        if use_risk_overlay and CFG.RISK_OVERLAY.get("enabled", True):
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
        self.log_lines.append(f"INFO: Backtest run_with_next_month_allocation gestartet.")
        self.log_lines.append(f"INFO: User Start Date: {self.user_start_date.strftime('%Y-%m-%d')}, Effective Start Date: {self.effective_start_date.strftime('%Y-%m-%d')}")

        # 1. Renditen vorbereiten
        returns = self.price_data.pct_change().fillna(0) # ffill(0) ersetzt auch initiale NaNs
        if returns.empty:
            self.log_lines.append("FEHLER: Preisdaten sind leer oder führen zu leeren Renditen.")
            # Setze leere Ergebnisse und beende
            self.portfolio_value = pd.Series(dtype=float, index=pd.to_datetime([]))
            self.daily_df = pd.DataFrame(columns=["Date", "Ticker", "Close Price", "Shares", "Allocated Amount", "PnL", "Is_Rebalance_Day", "Trading Costs"])
            self.monthly_allocations = pd.DataFrame()
            self._calculate_performance_metrics()
            return self.portfolio_value

        # 2. Rebalance-Zeitplan erstellen und filtern
        rebalance_schedule_full = build_rebalance_schedule(
            self.price_data, frequency=self.rebalance_freq, custom_months=self.custom_rebalance_months
        )
        
        rebalance_schedule = []
        if not returns.empty: # Sicherstellen, dass returns nicht leer ist
            returns_idx = returns.index
            for entry in rebalance_schedule_full:
                rebalance_event_date = entry["end_date"]
                if rebalance_event_date >= self.user_start_date:
                    actual_rebal_date_in_returns = rebalance_event_date
                    if actual_rebal_date_in_returns not in returns_idx:
                        past_dates = returns_idx[returns_idx <= actual_rebal_date_in_returns]
                        if not past_dates.empty: actual_rebal_date_in_returns = past_dates[-1]
                        else: continue
                    
                    if actual_rebal_date_in_returns in returns_idx:
                        window_end_loc = returns_idx.get_loc(actual_rebal_date_in_returns)
                        if window_end_loc >= self.window_days - 1:
                            entry["end_date"] = actual_rebal_date_in_returns # Wichtig: Datum anpassen
                            rebalance_schedule.append(entry)
        
        if not rebalance_schedule:
            msg = f"❌ Kein gültiger Rebalance-Termin gefunden am oder nach {self.user_start_date.strftime('%Y-%m-%d')} mit genügend Historie ({self.window_days} Tage)."
            print(msg); self.log_lines.append(msg)
            self.portfolio_value = pd.Series(dtype=float, index=pd.to_datetime([]))
            self.daily_df = pd.DataFrame(columns=["Date", "Ticker", "Close Price", "Shares", "Allocated Amount", "PnL", "Is_Rebalance_Day", "Trading Costs"])
            self._calculate_performance_metrics(); return self.portfolio_value

        # 3. Initialisierungen für den Backtest-Loop
        self.current_cash = self.start_balance
        backtest_relevant_dates = self.price_data.loc[self.user_start_date:].index # Nur Daten ab User-Start
        portfolio_values = pd.Series(index=backtest_relevant_dates, dtype=float)
        
        current_positions: Dict[str, Dict[str, Any]] = {}
        daily_log_list: List[Dict[str, Any]] = []
        monthly_alloc_log_list: List[Dict[str, Any]] = []
        self.total_trading_costs = 0.0
        last_known_prices: Dict[str, float] = {}

        # Fülle Portfolio-Wert für Tage vor dem ersten Rebalancing
        if not backtest_relevant_dates.empty:
            first_rebal_date = rebalance_schedule[0]['end_date']
            pre_rebal_days = backtest_relevant_dates[backtest_relevant_dates < first_rebal_date]
            if not pre_rebal_days.empty:
                portfolio_values.loc[pre_rebal_days] = self.start_balance

        # 4. Haupt-Backtest-Schleife über Rebalancing-Perioden
        for schedule_entry in rebalance_schedule:
            period_calc_start_date = schedule_entry["start_date"] # Für tägliche PnL
            rebalance_action_date = schedule_entry["end_date"]   # Tag des Rebalancings

            # Tage für die tägliche Wertentwicklung in diesem Segment (ab User-Start)
            segment_tracking_start = max(period_calc_start_date, self.user_start_date)
            days_in_current_segment = self.price_data.loc[segment_tracking_start:rebalance_action_date].index

            # 4.1 Tägliche Portfolio-Wertentwicklung innerhalb des Segments
            for day_num_in_segment, current_day in enumerate(days_in_current_segment):
                if current_day < self.user_start_date: continue

                current_day_equity_value = 0.0
                temp_last_prices_today: Dict[str, float] = {}

                for ticker, position_data in current_positions.items():
                    if ticker in self.price_data.columns and current_day in self.price_data.index:
                        price_today = self.price_data.at[current_day, ticker]
                        shares_held = position_data.get("shares", 0)

                        if pd.notna(price_today) and shares_held > 0:
                            current_day_equity_value += shares_held * price_today
                            
                            price_yesterday = last_known_prices.get(ticker)
                            pnl_for_ticker_today = 0.0
                            if price_yesterday is not None and pd.notna(price_yesterday):
                                pnl_for_ticker_today = (price_today - price_yesterday) * shares_held
                            
                            temp_last_prices_today[ticker] = price_today
                            
                            is_rebalance_action_day = (current_day == rebalance_action_date and day_num_in_segment == len(days_in_current_segment) -1)
                            daily_log_list.append({
                                "Date": current_day, "Ticker": ticker, "Close Price": price_today,
                                "Shares": shares_held, "Allocated Amount": shares_held * price_today,
                                "PnL": pnl_for_ticker_today, 
                                "Is_Rebalance_Day": is_rebalance_action_day,
                                "Trading Costs": position_data.get("trading_costs_today", 0.0) # Kosten nur am Rebalancing-Tag
                            })
                            position_data["trading_costs_today"] = 0.0 # Reset für den nächsten Tag
                
                last_known_prices.update(temp_last_prices_today)
                portfolio_values.loc[current_day] = current_day_equity_value + self.current_cash

            # 4.2 Rebalancing-Aktion am Ende des Segments (rebalance_action_date)
            if rebalance_action_date in self.price_data.index and rebalance_action_date >= self.user_start_date:
                portfolio_val_pre_rebal = portfolio_values.get(rebalance_action_date, self.current_cash + current_day_equity_value) # type: ignore
                
                returns_window_idx = returns.index.get_loc(rebalance_action_date)
                sub_returns = returns.iloc[returns_window_idx - self.window_days + 1 : returns_window_idx + 1]
                
                min_days_for_valid = int(self.window_days * 0.90)
                valid_tickers_for_rebal = [col for col in sub_returns.columns if sub_returns[col].count() >= min_days_for_valid]
                
                if not valid_tickers_for_rebal:
                    self.log_lines.append(f"WARNUNG: Keine validen Ticker für Rebalancing am {rebalance_action_date}. Positionen werden gehalten.")
                    continue

                num_available_for_selection = len(valid_tickers_for_rebal)
                
                # Standard-Ziel: 100% Aktien, gleichgewichtet über verfügbare Ticker
                target_equity_alloc_quote = 1.0 
                relative_equity_weights = {t: 1.0 / num_available_for_selection for t in valid_tickers_for_rebal if num_available_for_selection > 0}

                # --- Risk Overlay Anwendung (falls aktiviert) ---
                if self.risk_overlay:
                    # ... (Deine Risk Overlay Logik zur Anpassung von target_equity_alloc_quote und relative_equity_weights) ...
                    # Diese Logik habe ich aus deinem vorherigen Code übernommen.
                    day_df_overlay = pd.DataFrame(index=[rebalance_action_date])
                    if valid_tickers_for_rebal: day_df_overlay["close"] = self.price_data.loc[rebalance_action_date, valid_tickers_for_rebal].mean()
                    else: day_df_overlay["close"] = 0 
                    day_df_overlay["sentiment"] = 0.0 # Dummy
                    base_orders_overlay = {t: 1.0 / num_available_for_selection for t in valid_tickers_for_rebal if num_available_for_selection > 0}
                    try:
                        overlay_adj_weights = self.risk_overlay.apply(date=rebalance_action_date, data=day_df_overlay, base_orders=base_orders_overlay)
                        target_equity_alloc_quote = sum(overlay_adj_weights.values())
                        if target_equity_alloc_quote > 1e-6: # Kleine Schwelle
                            relative_equity_weights = {t: w / target_equity_alloc_quote for t, w in overlay_adj_weights.items()}
                        else: relative_equity_weights = {t: 0 for t in valid_tickers_for_rebal}
                    except Exception as e_overlay:
                        self.log_lines.append(f"FEHLER RiskOverlay.apply: {e_overlay}. Verwende 100% Aktien.")
                        target_equity_alloc_quote = 1.0

                # --- Positionsanpassung basierend auf Zielaktienquote und relativen Gewichten ---
                if target_equity_alloc_quote < 1e-6: # Ziel ist praktisch Cash
                    value_of_pos_pre_liq = sum(pos.get("shares",0) * self.price_data.at[rebalance_action_date, t] for t, pos in current_positions.items() if t in self.price_data.columns and pd.notna(self.price_data.at[rebalance_action_date, t]))
                    costs_this_rebal_event, temp_alloc_log_liq = self._execute_liquidation(rebalance_action_date, current_positions, portfolio_val_pre_rebal)
                    current_positions = {} # Alle Positionen verkauft
                    monthly_alloc_log_list.extend(temp_alloc_log_liq)
                    
                    # Log Liquidation für selection_details
                    self.selection_details.append({
                        "Rebalance Date": pd.to_datetime(rebalance_action_date).strftime('%Y-%m-%d'),
                        "Actual Rebalance Day": pd.to_datetime(rebalance_action_date).strftime('%Y-%m-%d'),
                        "Top Universe Size": num_available_for_selection,
                        "Selected Tickers": "CASH (Full Liquidation)",
                        "Optimization Method": "N/A", "Cov Estimator": "N/A", "Rebalance Frequency": self.rebalance_freq,
                        "Total Trading Costs": costs_this_rebal_event,
                        "Trading Costs %": (costs_this_rebal_event / value_of_pos_pre_liq * 100) if value_of_pos_pre_liq > 0 else 0.0
                    })
                else: # Normales Rebalancing
                    n_stocks_final_selection = min(self.num_stocks, num_available_for_selection)
                    optimizer_output_weights = pd.Series(dtype=float)
                    tickers_from_optimizer = []

                    if self.optimization_mode == "select-then-optimize":
                        top_sharpe_tickers = select_top_sharpe_tickers(sub_returns[valid_tickers_for_rebal], n_stocks_final_selection)
                        if not top_sharpe_tickers.empty:
                            optimizer_output_weights = optimize_portfolio(returns=sub_returns[top_sharpe_tickers], method=self.optimizer_method, cov_estimator=self.cov_estimator, min_weight=self.min_weight, max_weight=self.max_weight, force_equal_weight=self.force_equal_weight, num_stocks=len(top_sharpe_tickers))
                            tickers_from_optimizer = top_sharpe_tickers.tolist()
                    elif self.optimization_mode == "optimize-subset":
                        optimizer_output_weights = optimize_portfolio(returns=sub_returns[valid_tickers_for_rebal], method=self.optimizer_method, cov_estimator=self.cov_estimator, min_weight=self.min_weight, max_weight=self.max_weight, force_equal_weight=self.force_equal_weight, num_stocks=n_stocks_final_selection)
                        tickers_from_optimizer = optimizer_output_weights.index.tolist()
                    
                    if optimizer_output_weights.empty or optimizer_output_weights.sum() < 0.99:
                        self.log_lines.append(f"WARNUNG: Optimizer ergab keine gültigen Gewichte am {rebalance_action_date}. Halte Positionen."); continue

                    # Kombiniere Overlay-relative Gewichte mit Optimizer-Gewichten
                    final_target_weights_dict = {
                        t: relative_equity_weights.get(t, 0) * optimizer_output_weights.get(t, 0)
                        for t in tickers_from_optimizer if t in relative_equity_weights
                    }
                    final_target_weights_series = pd.Series(final_target_weights_dict).loc[lambda x: x > 1e-6]

                    if final_target_weights_series.empty or final_target_weights_series.sum() == 0:
                        self.log_lines.append(f"WARNUNG: Keine validen kombinierten Gewichte am {rebalance_action_date}. Halte Positionen."); continue
                    
                    # Normalisiere die kombinierten Gewichte, sodass ihre Summe 1 ergibt (innerhalb des Aktienanteils)
                    final_target_weights_series /= final_target_weights_series.sum() 
                    
                    # Tatsächliche Allokation basierend auf Zielaktienquote und normalisierten Gewichten
                    investable_amount_for_equity = portfolio_val_pre_rebal * target_equity_alloc_quote
                    
                    current_positions, alloc_log_for_rebal = allocate_positions(
                        self.price_data, final_target_weights_series.index.tolist(), final_target_weights_series.values,
                        rebalance_action_date, investable_amount_for_equity, current_positions.copy(),
                        self.enable_trading_costs, self.fixed_cost_per_trade, self.variable_cost_pct
                    )
                    
                    invested_value_this_rebal = sum(alloc.get("Value", 0.0) for alloc in alloc_log_for_rebal if alloc.get("Ticker") != "TOTAL_COSTS")
                    costs_this_rebal_event = next((alloc.get("Trading Costs", 0.0) for alloc in alloc_log_for_rebal if alloc.get("Ticker") == "TOTAL_COSTS"), 0.0)
                    
                    self.current_cash = portfolio_val_pre_rebal - invested_value_this_rebal - costs_this_rebal_event
                    self.total_trading_costs += costs_this_rebal_event
                    monthly_alloc_log_list.extend(alloc_log_for_rebal)

                    # --- Logging für selection_details (Normales Rebalancing) ---
                    self.selection_details.append({
                        "Rebalance Date": pd.to_datetime(rebalance_action_date).strftime('%Y-%m-%d'),
                        "Actual Rebalance Day": pd.to_datetime(rebalance_action_date).strftime('%Y-%m-%d'),
                        "Top Universe Size": num_available_for_selection,
                        "Selected Tickers": ", ".join(sorted(final_target_weights_series.index.tolist())),
                        "Optimization Method": self.optimizer_method, "Cov Estimator": self.cov_estimator,
                        "Rebalance Frequency": self.rebalance_freq,
                        "Total Trading Costs": costs_this_rebal_event,
                        "Trading Costs %": (costs_this_rebal_event / portfolio_val_pre_rebal * 100) if portfolio_val_pre_rebal and portfolio_val_pre_rebal > 0 else 0.0,
                        # Optional: "Target Equity Quote": target_equity_alloc_quote*100
                    })

                # Aktualisiere Portfoliowert am Rebalancing-Tag
                if rebalance_action_date in portfolio_values.index:
                    final_equity_after_rebal = sum(pos.get("shares",0) * self.price_data.at[rebalance_action_date, t] for t, pos in current_positions.items() if t in self.price_data.columns and pd.notna(self.price_data.at[rebalance_action_date, t]))
                    portfolio_values.loc[rebalance_action_date] = final_equity_after_rebal + self.current_cash
        
        # 5. Finale Datenaufbereitung und Metriken
        self.portfolio_value = portfolio_values.dropna()
        self.daily_df = pd.DataFrame(daily_log_list)
        if not self.daily_df.empty and not self.portfolio_value.empty and "Date" in self.daily_df.columns:
            self.daily_df["Date"] = pd.to_datetime(self.daily_df["Date"])
            pv_for_merge = self.portfolio_value.rename("Total Portfolio Value").reset_index()
            pv_for_merge.columns = ["Date", "Total Portfolio Value"] # Stelle Spaltennamen sicher
            pv_for_merge["Date"] = pd.to_datetime(pv_for_merge["Date"])
            merged_df = pd.merge(self.daily_df, pv_for_merge, on="Date", how="left")
            if "Total Portfolio Value" in merged_df.columns and "Allocated Amount" in merged_df.columns:
                merged_df["Allocated Percentage (%)"] = (merged_df["Allocated Amount"] / merged_df["Total Portfolio Value"] * 100).fillna(0)
                self.daily_df = merged_df.drop(columns=["Total Portfolio Value"], errors='ignore')

        self.monthly_allocations = pd.DataFrame(monthly_alloc_log_list)
        
        # Füge die SUMMARY-Zeile zu selection_details hinzu
        self.selection_details.append({
            "Rebalance Date": "SUMMARY", "Actual Rebalance Day": "SUMMARY",
            "Top Universe Size": 0, "Selected Tickers": "N/A", "Optimization Method": "N/A", 
            "Cov Estimator": "N/A", "Rebalance Frequency": self.rebalance_freq,
            "Total Trading Costs": self.total_trading_costs,
            "Trading Costs %": (self.total_trading_costs / self.start_balance * 100) if self.start_balance > 0 else 0.0
        })

        # KORREKTUR: Logik für Next-Month Allocation
        self.next_month_tickers = []
        self.next_month_weights = pd.Series(dtype=float)

        if not returns.empty:
            last_data_date_for_returns = returns.index.max() # Letztes Datum im Returns-Index
            
            # Stelle sicher, dass das Datum auch im Preisdatenindex für die Overlay-Indikatoren existiert
            if last_data_date_for_returns in self.price_data.index:
                nm_window_end_idx = returns.index.get_loc(last_data_date_for_returns)

                if nm_window_end_idx >= self.window_days - 1:
                    nm_sub_returns = returns.iloc[nm_window_end_idx - self.window_days + 1 : nm_window_end_idx + 1]
                    
                    nm_min_valid_days = int(self.window_days * 0.90)
                    nm_valid_tickers_in_sub = [
                        col for col in nm_sub_returns.columns if nm_sub_returns[col].count() >= nm_min_valid_days
                    ]

                    if nm_valid_tickers_in_sub:
                        available_nm = len(nm_valid_tickers_in_sub)
                        n_stocks_nm = min(self.num_stocks, available_nm)
                        
                        # Zielaktienquote für Next Month (optional, hier als 100% angenommen, wenn Overlay nicht detailliert für Prognose verwendet wird)
                        nm_target_equity_exposure = 1.0
                        nm_equity_weights_normalized_to_one = {t: 1/available_nm for t in nm_valid_tickers_in_sub}

                        if self.risk_overlay: # Optional: Overlay auch für Next-Month-Prognose verwenden
                            nm_day_df_for_overlay = pd.DataFrame(index=[last_data_date_for_returns])
                            nm_day_df_for_overlay["close"] = self.price_data.loc[last_data_date_for_returns, nm_valid_tickers_in_sub].mean()
                            nm_day_df_for_overlay["sentiment"] = 0.0 # Dummy
                            
                            nm_base_orders = {t: 1/available_nm for t in nm_valid_tickers_in_sub}
                            try:
                                nm_overlay_scaled_weights = self.risk_overlay.apply(date=last_data_date_for_returns, data=nm_day_df_for_overlay, base_orders=nm_base_orders)
                                nm_target_equity_exposure = sum(nm_overlay_scaled_weights.values())
                                if nm_target_equity_exposure > 0:
                                    nm_equity_weights_normalized_to_one = {t: w / nm_target_equity_exposure for t, w in nm_overlay_scaled_weights.items()}
                                else:
                                    nm_equity_weights_normalized_to_one = {t: 0 for t in nm_valid_tickers_in_sub}
                            except Exception as e:
                                print(f"FEHLER bei RiskOverlay.apply für Next Month: {e}")
                        
                        if nm_target_equity_exposure > 0: # Nur optimieren, wenn investiert werden soll
                            nm_final_optimizer_weights = pd.Series(dtype=float)
                            nm_selected_tickers_for_portfolio = []

                            if self.optimization_mode == "select-then-optimize":
                                nm_top_tickers_idx = select_top_sharpe_tickers(nm_sub_returns[nm_valid_tickers_in_sub], n_stocks_nm)
                                if not nm_top_tickers_idx.empty:
                                    nm_final_optimizer_weights = optimize_portfolio(
                                        returns=nm_sub_returns[nm_top_tickers_idx],
                                        method=self.optimizer_method, cov_estimator=self.cov_estimator,
                                        min_weight=self.min_weight, max_weight=self.max_weight,
                                        force_equal_weight=self.force_equal_weight,
                                        debug_label="NextMonth A (select-then-optimize)",
                                        num_stocks=len(nm_top_tickers_idx))
                                    nm_selected_tickers_for_portfolio = nm_top_tickers_idx.tolist()
                            
                            elif self.optimization_mode == "optimize-subset":
                                nm_final_optimizer_weights = optimize_portfolio(
                                    returns=nm_sub_returns[nm_valid_tickers_in_sub],
                                    method=self.optimizer_method, cov_estimator=self.cov_estimator,
                                    min_weight=self.min_weight, max_weight=self.max_weight,
                                    force_equal_weight=self.force_equal_weight,
                                    debug_label="NextMonth B (optimize-subset)",
                                    num_stocks=n_stocks_nm)
                                nm_selected_tickers_for_portfolio = nm_final_optimizer_weights.index.tolist()

                            if not nm_final_optimizer_weights.empty:
                                nm_combined_weights = {
                                    t: nm_equity_weights_normalized_to_one.get(t, 0) * nm_final_optimizer_weights.get(t, 0)
                                    for t in nm_selected_tickers_for_portfolio
                                    if t in nm_equity_weights_normalized_to_one
                                }
                                nm_final_series = pd.Series(nm_combined_weights).loc[lambda x: x > 1e-6]
                                if not nm_final_series.empty and nm_final_series.sum() > 0:
                                    self.next_month_weights = (nm_final_series / nm_final_series.sum()) * nm_target_equity_exposure # Skaliert mit Zielquote
                                    self.next_month_tickers = self.next_month_weights.index.tolist()
                                else: # Keine validen Gewichte
                                     self.next_month_weights = pd.Series(dtype=float)
                                     self.next_month_tickers = []
                            else: # Optimizer lieferte keine Gewichte
                                self.next_month_weights = pd.Series(dtype=float)
                                self.next_month_tickers = []
                        else: # Zielaktienquote für Next Month ist 0
                            self.next_month_weights = pd.Series(dtype=float) # Keine Gewichtung
                            self.next_month_tickers = [] # Keine Ticker
        
        print("🔮 Next-Month-Universe:", getattr(self, "next_month_tickers", []))
        
        self._calculate_performance_metrics()

        # Ergebnisse auf Backtest-Start zuschneiden (self.user_start_date)
        if not self.portfolio_value.empty:
             self.portfolio_value = self.portfolio_value.loc[self.user_start_date:]
        if not self.daily_df.empty:
            self.daily_df = self.daily_df[self.daily_df["Date"] >= self.user_start_date].reset_index(drop=True)
        
        # true_daily_portfolio_pnl wird in _calculate_performance_metrics gesetzt
        if hasattr(self, 'true_daily_portfolio_pnl') and not self.true_daily_portfolio_pnl.empty:
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
        if self.portfolio_value.empty or "Date" not in self.daily_df.columns:
            self.performance_metrics   = pd.DataFrame()
            self.monthly_performance   = pd.DataFrame()
            self.true_daily_portfolio_pnl = pd.Series(dtype=float)
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
        if self.daily_df.empty or "Date" not in self.daily_df.columns:
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