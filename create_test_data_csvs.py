# create_test_data_csvs.py
import yfinance as yf
import pandas as pd
import os

# --- Konfiguration ---
tickers_to_download = {
    "SPY": "SPY",      # Für SPY-bezogene Indikatoren
    "VIX": "^VIX",     # Für VIX-bezogene Indikatoren
    # Füge hier weitere Ticker hinzu, wenn deine Indikatoren sie benötigen,
    # z.B. für Gold ("GLD"), Anleihen ("AGG", "SHY"), oder spezifische Sektoren.
    # Für Sentiment-Daten (AAII) oder Makrodaten (Zinskurve) brauchst du andere Quellen.
    # Diese CSVs musst du manuell erstellen oder von einer API beziehen.
}
start_date = "2010-01-01"
end_date = pd.Timestamp.today().strftime('%Y-%m-%d') # Bis heute
project_root = os.path.dirname(os.path.abspath(__file__)) # Annahme: Skript ist im Projekt-Root

# --- Download und Speichern ---
for asset_key, yf_ticker in tickers_to_download.items():
    print(f"Lade Daten für {asset_key} (Ticker: {yf_ticker})...")
    try:
        data = yf.download(yf_ticker, start=start_date, end=end_date)
        if not data.empty:
            # yfinance gibt Spaltennamen mit Großbuchstaben zurück, das passt.
            # Stelle sicher, dass die Spalten 'Open', 'High', 'Low', 'Close', 'Volume' vorhanden sind.
            # 'Adj Close' wird oft verwendet, aber für TA sind oft die nicht-adjustierten Preise besser.
            # Wir verwenden hier die Standardspalten von yfinance.
            
            # Umbenennen, falls nötig, damit sie zu den Erwartungen passen (O,H,L,C,V)
            # yfinance verwendet bereits diese Namen.
            
            output_filename = os.path.join(project_root, f"test_data_{asset_key.lower()}.csv")
            data.index.name = "Date" # Index benennen
            data.to_csv(output_filename)
            print(f"Daten für {asset_key} gespeichert in {output_filename}")
        else:
            print(f"Keine Daten für {asset_key} (Ticker: {yf_ticker}) gefunden.")
    except Exception as e:
        print(f"Fehler beim Download für {asset_key} (Ticker: {yf_ticker}): {e}")

print("\n--- Erstellung der Testdaten-CSVs abgeschlossen ---")
print("WICHTIG: Für Indikatoren wie AAII-Sentiment oder Zinskurven musst du die")
print("entsprechenden CSV-Dateien manuell erstellen oder aus anderen Quellen beziehen,")
print("z.B. 'test_data_aaii.csv' oder 'test_data_yields.csv' und die Spaltennamen")
print("in den Indikator-Funktionen und der Config anpassen.")