if self.optimization_mode == "select-then-optimize":
                        # Wähle die Top n_stocks direkt aus den validen Tickern des Sub-Return-Fensters
                        top_tickers_selected_idx = select_top_sharpe_tickers(sub_returns[valid_tickers], n_stocks) #

                        if not top_tickers_selected_idx.empty: #
                            weights_series = optimize_portfolio(
                                returns=sub_returns[top_tickers_selected_idx], # KORREKTUR HIER
                                method=self.optimizer_method,
                                cov_estimator=self.cov_estimator,
                                min_weight=self.min_weight,
                                max_weight=self.max_weight,
                                force_equal_weight=self.force_equal_weight,
                                debug_label="A - Optimizer only weight",
                                num_stocks=len(top_tickers_selected_idx), # KORREKTUR HIER
                            )
                            top_tickers = top_tickers_selected_idx.tolist() # KORREKTUR HIER
                        else:
                            print(f"WARNUNG: Keine Ticker nach Sharpe-Selektion für {rebalance_date}. Halte Positionen.") #
                            continue #
                    else: # "optimize-subset"
                        weights_full = optimize_portfolio(
                            returns=sub_returns[valid_tickers], #
                            # ... (Rest bleibt gleich)
                            num_stocks=n_stocks, #
                        )
                        top_tickers = weights_full.index.tolist() #
                        weights_series = weights_full #

                    # Optimizer-Gewichte × Equity-Quote-Gewichte → Ziel-Gewichte
                    opt_weights    = weights_series.to_dict() #
                    target_weights = {t: equity_weights.get(t, 0) * opt_weights.get(t, 0)
                                    for t in top_tickers} # top_tickers ist jetzt korrekt
                    weights = np.array([target_weights[t] for t in top_tickers]) #
                    # ...