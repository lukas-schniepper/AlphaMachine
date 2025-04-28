#!/usr/bin/env python3
"""
check_columns.py

A script to inspect and adjust ticker info DataFrame columns,
avoid SettingWithCopyWarning, handle alias columns,
merge alias values into primary columns, and output cleaned DataFrame.
"""

import pandas as pd

# --- Hier: Ersetze fetch_ticker_info durch deine tatsächliche Funktion zum Holen der Daten
# from your_module import fetch_ticker_info

def fetch_ticker_info():
    """
    Platzhalter-Funktion. Ersetze dies durch deinen echten API-Call oder Datenabruf.
    """
    class Dummy:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    # Beispiel-Daten, teils unvollständig
    return [
        Dummy(id=1, ticker="ABC", sector="Tech", currency="EUR", actual_start_date="2020-01-01", last_update="2025-04-27"),
        Dummy(id=2, symbol="XYZ", sect="Finance")  # bewusst unvollständig
    ]


def main():
    # 1. Daten abrufen
    info = fetch_ticker_info()

    # 2. Roh-DataFrame erzeugen und Spalten prüfen
    df_raw = pd.DataFrame([vars(i) for i in info])
    print("Verfügbare Spalten (roh):", df_raw.columns.tolist())

    # 3. Alias-Werte in Primärspalten übertragen
    if 'symbol' in df_raw.columns:
        df_raw['ticker'] = df_raw.get('ticker').fillna(df_raw['symbol'])
    if 'sect' in df_raw.columns:
        df_raw['sector'] = df_raw.get('sector').fillna(df_raw['sect'])

    # 4. Alias-Spalten entfernen
    drop_cols = [c for c in ('symbol', 'sect') if c in df_raw.columns]
    df_clean = df_raw.drop(columns=drop_cols)

    print("Verfügbare Spalten (bereinigt):", df_clean.columns.tolist())

    # 5. Gewünschte Spalten definieren und DataFrame neu indexieren
    wanted = ['id', 'ticker', 'sector', 'currency', 'actual_start_date', 'actual_end_date', 'last_update']
    dfi = df_clean.reindex(columns=wanted)

    # 6. Ergebnis ausgeben
    print("\nErgebnis-DataFrame:")
    print(dfi)


if __name__ == "__main__":
    main()
