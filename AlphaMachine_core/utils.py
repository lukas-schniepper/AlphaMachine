import pandas as pd
import numpy as np


def build_rebalance_schedule(price_data, frequency="monthly", custom_months=1):
    """
    Erstellt einen Rebalancing-Zeitplan basierend auf der angegebenen Frequenz.

    Args:
        price_data: Preisdaten als pandas DataFrame
        frequency: "weekly", "monthly" oder custom (Anzahl der Monate)
        custom_months: Anzahl der Monate zwischen Rebalances, wenn frequency weder "weekly" noch "monthly" ist

    Returns:
        Liste von Rebalancing-Zeitpunkten
    """
    schedule = []

    if frequency == "weekly":
        # Wöchentliches Rebalancing (Ende jeder Woche)
        week_ends = price_data.groupby(price_data.index.to_period("W")).apply(
            lambda x: x.index[-1]
        )
        week_ends = pd.to_datetime(week_ends.values)

        for i in range(len(week_ends) - 1):
            rebalance_day = week_ends[i]
            start_date = week_ends[i] + pd.Timedelta(days=1)
            end_date = week_ends[i + 1]
            if not price_data.loc[start_date:end_date].empty:
                schedule.append(
                    {
                        "rebalance_date": rebalance_day,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
    else:
        # Monatliches oder benutzerdefiniertes Rebalancing
        month_ends = price_data.groupby(price_data.index.to_period("M")).apply(
            lambda x: x.index[-1]
        )
        month_ends = pd.to_datetime(month_ends.values)

        if frequency == "monthly":
            step = 1
        else:
            step = custom_months  # Benutzerdefinierte Anzahl von Monaten

        # Erstelle eine gefilterte Liste von Monatsenden basierend auf dem Schritt
        rebalance_months = [month_ends[i] for i in range(0, len(month_ends), step)]

        for i in range(len(rebalance_months) - 1):
            rebalance_day = price_data[
                price_data.index.to_period("M") == rebalance_months[i].to_period("M")
            ].index.max()
            # Der Starttag ist der Tag nach dem Rebalancing-Tag
            start_date = rebalance_day + pd.Timedelta(days=1)

            # Das Ende ist der nächste Rebalancing-Tag
            if i + 1 < len(rebalance_months):
                next_rebalance_day = price_data[
                    price_data.index.to_period("M")
                    == rebalance_months[i + 1].to_period("M")
                ].index.max()
                end_date = next_rebalance_day
            else:
                end_date = month_ends[-1]

            if not price_data.loc[start_date:end_date].empty:
                schedule.append(
                    {
                        "rebalance_date": rebalance_day,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )

    return schedule


def select_top_sharpe_tickers(returns, top_n):
    sharpe_scores = returns.mean() / returns.std()
    sharpe_scores = sharpe_scores.replace([np.inf, -np.inf], np.nan).dropna()
    top_universe = sharpe_scores.sort_values(ascending=False).head(top_n).index
    return top_universe


def allocate_positions(
    price_data,
    tickers,
    weights,
    date,
    balance,
    previous_positions=None,
    enable_trading_costs=False,
    fixed_cost_per_trade=0.0,
    variable_cost_pct=0.0,
):
    """
    Verteilt das Portfolio-Vermögen auf die verschiedenen Titel unter Berücksichtigung von Trading-Kosten.

    Args:
        price_data: Preisdaten als pandas DataFrame
        tickers: Liste der Ticker-Symbole
        weights: Liste der Gewichtungen
        date: Datum des Rebalancings
        balance: Aktuelles Portfolio-Vermögen
        previous_positions: Vorherige Positionen (optional)
        enable_trading_costs: Trading-Kosten aktivieren/deaktivieren
        fixed_cost_per_trade: Fixer Kostenbetrag pro Trade
        variable_cost_pct: Variable Kosten als Prozentsatz des Handelsvolumens

    Returns:
        positions: Dictionary mit den neuen Positionen
        allocations: Liste mit Allokations-Informationen
    """
    positions = {}
    allocations = []
    total_costs = 0.0

    # Wenn Trading-Kosten aktiviert sind, schätzen wir die Anzahl der Trades ab
    if enable_trading_costs:
        # Anzahl der Trades = Anzahl der neuen Positionen + Anzahl der zu schließenden Positionen
        estimated_trade_count = len(tickers)
        if previous_positions:
            # Zusätzliche Trades für Positionen, die geschlossen werden müssen
            positions_to_close = set(previous_positions.keys()) - set(tickers)
            estimated_trade_count += len(positions_to_close)

        # Reduziere das verfügbare Kapital um die geschätzten fixen Kosten
        estimated_fixed_costs = estimated_trade_count * fixed_cost_per_trade
        available_balance = balance - estimated_fixed_costs

        # Stellen wir sicher, dass nach Abzug der fixen Kosten noch Geld übrig ist
        if available_balance <= 0:
            print(
                f"⚠️ Warnung: Geschätzte Trading-Kosten ({estimated_fixed_costs:.2f}) übersteigen das verfügbare Kapital ({balance:.2f})!"
            )
            available_balance = (
                balance * 0.9
            )  # Wir verwenden 90% des Kapitals für Positionen
    else:
        available_balance = balance

    # Vorherige Positionen für Berechnung der Trading-Kosten speichern
    prev_shares = {}
    if previous_positions:
        for ticker, pos in previous_positions.items():
            prev_shares[ticker] = pos["shares"]

    # Allokiere Vermögen auf die Positionen
    for i, ticker in enumerate(tickers):
        price = price_data.loc[date, ticker]
        if np.isnan(price):
            continue

        allocation = available_balance * weights[i]
        shares = allocation / price

        # Trading-Kosten berechnen
        trading_costs = 0.0
        if enable_trading_costs:
            old_shares = prev_shares.get(ticker, 0)
            shares_change = abs(shares - old_shares)

            if shares_change > 0:
                # Fixer Kostenbetrag pro Trade
                trading_costs += fixed_cost_per_trade
                # Variable Kosten basierend auf dem Handelsvolumen
                trading_value = shares_change * price
                trading_costs += trading_value * variable_cost_pct

        total_costs += trading_costs

        positions[ticker] = {
            "shares": shares,
            "cost_basis": price,
            "value": allocation,
            "weight": weights[i],
            "trading_costs": trading_costs,
        }

        allocations.append(
            {
                "Rebalance Date": date.date(),
                "Ticker": ticker,
                "Weight (%)": weights[i] * 100,
                "Shares": shares,
                "Price": price,
                "Value": allocation,
                "Trading Costs": trading_costs,
            }
        )

    # Trading-Kosten für Positionen, die geschlossen werden müssen
    if enable_trading_costs and previous_positions:
        for ticker, pos in previous_positions.items():
            if ticker not in tickers:  # Diese Position wird geschlossen
                trading_costs = fixed_cost_per_trade
                trading_costs += pos["shares"] * pos["cost_basis"] * variable_cost_pct
                total_costs += trading_costs

                allocations.append(
                    {
                        "Rebalance Date": date.date(),
                        "Ticker": ticker,
                        "Weight (%)": 0,
                        "Shares": 0,
                        "Price": pos[
                            "cost_basis"
                        ],  # Wir haben keinen aktuellen Preis, also verwenden wir den letzten bekannten
                        "Value": 0,
                        "Trading Costs": trading_costs,
                        "Action": "Position Closed",
                    }
                )

    if enable_trading_costs:
        allocations.append(
            {
                "Rebalance Date": date.date(),
                "Ticker": "TOTAL_COSTS",
                "Weight (%)": (total_costs / balance) * 100 if balance > 0 else 0,
                "Shares": None,
                "Price": None,
                "Value": total_costs,
                "Trading Costs": total_costs,
                "Action": "Summary",
            }
        )

    return positions, allocations
