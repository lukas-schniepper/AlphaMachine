import yfinance as yf
import pandas as pd
import numpy as np
import time
import os
import datetime as dt


class StockDataManager:
    """
    Verbesserter Data Manager für Aktien-Backtests mit standardisierten CSV-Dateien
    """
    def __init__(self, base_folder = os.path.expanduser("~/data_alpha")):
        self.base_folder = base_folder
        self.price_data_folder = os.path.join(base_folder, 'price_data')
        self.ticker_periods_file = os.path.join(base_folder, 'ticker_periods.csv')
        self.ticker_info_file = os.path.join(base_folder, 'ticker_info.csv')

        # Verzeichnisse erstellen
        os.makedirs(self.base_folder, exist_ok=True)
        os.makedirs(self.price_data_folder, exist_ok=True)

        # Control Files initialisieren
        self._init_ticker_periods()
        self._init_ticker_info()

        # Für Fehlerverfolgung
        self.skipped_tickers = []

    def _init_ticker_periods(self):
        """Initialisiert die Ticker-Perioden-Datei falls nicht vorhanden"""
        if not os.path.exists(self.ticker_periods_file):
            periods = pd.DataFrame(columns=['ticker', 'start_date', 'end_date', 'source'])
            periods.to_csv(self.ticker_periods_file, index=False)

    def _init_ticker_info(self):
        """Initialisiert die Ticker-Info-Datei falls nicht vorhanden"""
        if not os.path.exists(self.ticker_info_file):
            info = pd.DataFrame(columns=['ticker', 'sector', 'currency', 'actual_start_date', 'actual_end_date', 'last_update'])
            info.to_csv(self.ticker_info_file, index=False)

    def add_tickers_for_period(self, tickers, period_start_date, period_end_date=None, source_name="manual"):
        """
        Fügt Ticker für einen bestimmten Zeitraum hinzu

        Parameters:
        -----------
        tickers : list
            Liste der Ticker-Symbole
        period_start_date : str
            Startdatum für die Periode (YYYY-MM-DD)
        period_end_date : str, optional
            Enddatum für die Periode (YYYY-MM-DD)
        source_name : str, optional
            Name der Quelle der Ticker-Liste
        """
        if period_end_date is None:
            # Default: Ende des Monats vom Startdatum
            start_dt = pd.to_datetime(period_start_date)
            period_end_date = (start_dt + pd.offsets.MonthEnd(1)).strftime('%Y-%m-%d')

        print(f"\n=== Füge {len(tickers)} Ticker für Periode {period_start_date} bis {period_end_date} hinzu ===")

        # Ticker-Perioden-Datei laden
        if os.path.exists(self.ticker_periods_file):
            periods = pd.read_csv(self.ticker_periods_file)
        else:
            periods = pd.DataFrame(columns=['ticker', 'start_date', 'end_date', 'source'])

        # Für jeden Ticker einen Eintrag hinzufügen
        new_entries = []
        for ticker in tickers:
            # Prüfen, ob dieser Eintrag bereits existiert
            exists = ((periods['ticker'] == ticker) &
                     (periods['start_date'] == period_start_date) &
                     (periods['end_date'] == period_end_date)).any()

            if not exists:
                new_entries.append({
                    'ticker': ticker,
                    'start_date': period_start_date,
                    'end_date': period_end_date,
                    'source': source_name
                })
                print(f"  + {ticker} hinzugefügt")
            else:
                print(f"  ℹ️ {ticker} bereits für diese Periode definiert")

        # Neue Einträge hinzufügen
        if new_entries:
            new_df = pd.DataFrame(new_entries)
            periods = pd.concat([periods, new_df], ignore_index=True)
            periods.to_csv(self.ticker_periods_file, index=False)
            print(f"  ✅ {len(new_entries)} neue Ticker-Perioden gespeichert")

        return tickers

    def update_ticker_data(self, tickers=None, history_start='1990-01-01'):
        """
        Aktualisiert die Daten für die angegebenen Ticker
        Macht entweder einen Delta-Load oder einen Erstdownload

        Parameters:
        -----------
        tickers : list, optional
            Liste der zu aktualisierenden Ticker. Wenn None, werden alle
            in ticker_periods.csv definierten Ticker aktualisiert.
        history_start : str, optional
            Startdatum für historische Daten bei Tickern, die erstmals
            heruntergeladen werden (YYYY-MM-DD)
        """
        # Wenn keine Ticker angegeben, alle aus ticker_periods nehmen
        if tickers is None:
            if os.path.exists(self.ticker_periods_file):
                periods_df = pd.read_csv(self.ticker_periods_file)
                tickers = periods_df['ticker'].unique().tolist()
                print(f"\n=== Aktualisiere Daten für {len(tickers)} Ticker aus ticker_periods.csv ===")
            else:
                print("⚠️ Keine Ticker spezifiziert und keine ticker_periods.csv gefunden!")
                return []
        else:
            print(f"\n=== Aktualisiere Daten für {len(tickers)} angegebene Ticker ===")

        history_start_date = pd.to_datetime(history_start)
        end_date = pd.to_datetime(dt.date.today())
        updated_tickers = []

        for ticker in tickers:
            try:
                print(f"\n--- Verarbeite {ticker} ---")
                # Prüfen, ob Daten bereits existieren
                ticker_file = os.path.join(self.price_data_folder, f"{ticker}.csv")
                if os.path.exists(ticker_file):
                    # Delta-Load für vorhandene Daten
                    try:
                        # CSV richtig einlesen - mit klarem Format
                        existing_data = pd.read_csv(ticker_file, parse_dates=['date'])

                        last_date = existing_data['date'].max().date()
                        start_date = last_date + dt.timedelta(days=1)

                        if start_date >= dt.date.today():
                            print(f"  ✅ {ticker}: Daten bereits aktuell (letzter Tag: {last_date})")
                            updated_tickers.append(ticker)
                            continue

                        print(f"  🔄 {ticker}: Lade fehlende Daten von {start_date} bis heute")
                        new_data = self._download_ticker_data(ticker, start_date, end_date)

                        if new_data is not None and not new_data.empty:
                            # Neue Daten ins richtige Format bringen
                            new_data_formatted = self._prepare_price_data(ticker, new_data)

                            # Neue und bestehende Daten zusammenführen
                            combined_data = pd.concat([existing_data, new_data_formatted])

                            # Duplikate entfernen, falls vorhanden (basierend auf date)
                            combined_data.drop_duplicates(subset=['date'], keep='last', inplace=True)

                            # Nach Datum sortieren
                            combined_data.sort_values('date', inplace=True)

                            # Als saubere CSV speichern
                            combined_data.to_csv(ticker_file, index=False)

                            # Tatsächliches Startdatum und Enddatum für ticker_info
                            actual_start_date = combined_data['date'].min().date()
                            actual_end_date = combined_data['date'].max().date()

                            print(f"  ✅ {ticker}: {len(new_data_formatted)} neue Datenpunkte hinzugefügt")
                            print(f"  📅 {ticker}: Tatsächliches Startdatum: {actual_start_date}, Enddatum: {actual_end_date}")

                            # Ticker-Info mit Start- und Enddatum aktualisieren
                            self._update_ticker_info(ticker, actual_start_date, actual_end_date)

                            updated_tickers.append(ticker)
                        else:
                            # Startdatum und Enddatum aus vorhandenen Daten ermitteln
                            actual_start_date = existing_data['date'].min().date()
                            actual_end_date = existing_data['date'].max().date()

                            print(f"  ℹ️ {ticker}: Keine neuen Daten verfügbar")
                            print(f"  📅 {ticker}: Tatsächliches Startdatum: {actual_start_date}, Enddatum: {actual_end_date}")

                            # Ticker-Info mit Start- und Enddatum aktualisieren
                            self._update_ticker_info(ticker, actual_start_date, actual_end_date)

                            updated_tickers.append(ticker)
                    except Exception as e:
                        print(f"  ⚠️ {ticker}: Fehler beim Delta-Load: {e}")
                        # Versuche kompletten Neuladen bei Fehler
                        self._full_download(ticker, ticker_file, history_start_date, end_date)
                        updated_tickers.append(ticker)
                else:
                    # Erstdownload für neue Ticker
                    self._full_download(ticker, ticker_file, history_start_date, end_date)
                    updated_tickers.append(ticker)

                # Kurze Pause um API-Limits zu vermeiden
                time.sleep(0.2)

            except Exception as e:
                print(f"  ❌ {ticker}: Unerwarteter Fehler: {e}")
                self.skipped_tickers.append(ticker)

        # Zusammenfassung anzeigen
        print("\n=== Zusammenfassung der Datenaktualisierung ===")
        print(f"  ✅ {len(updated_tickers)} Ticker erfolgreich aktualisiert")
        if self.skipped_tickers:
            print(f"  ⚠️ {len(self.skipped_tickers)} Ticker übersprungen: {', '.join(self.skipped_tickers)}")
            self.skipped_tickers = []  # Liste zurücksetzen

        return updated_tickers

    def _prepare_price_data(self, ticker, data):
        """Kein Umbau mehr nötig – Daten sind bereits fertig"""
        return data[['date', 'ticker', 'close']]

    def _full_download(self, ticker, ticker_file, start_date, end_date):
        """Führt einen vollständigen Download für einen Ticker durch"""
        print(f"  📥 {ticker}: Vollständiger Download ({start_date.date()} bis {end_date.date()})")
        data = self._download_ticker_data(ticker, start_date, end_date)

        if data is not None and not data.empty:
            # Daten ins richtige Format bringen
            formatted_data = self._prepare_price_data(ticker, data)

            # Frühestes und spätestes Datum ermitteln
            actual_start_date = formatted_data['date'].min().date()
            actual_end_date = formatted_data['date'].max().date()

            # Tatsächliches Startdatum in der Konsole ausgeben
            print(f"  📅 {ticker}: Tatsächliches Startdatum: {actual_start_date}, Enddatum: {actual_end_date}")

            # Als saubere CSV speichern
            formatted_data.to_csv(ticker_file, index=False)

            # Ticker-Info mit Start- und Enddatum aktualisieren
            self._update_ticker_info(ticker, actual_start_date, actual_end_date)

            print(f"  ✅ {ticker}: {len(formatted_data)} Datenpunkte heruntergeladen")
            return True
        else:
            print(f"  ❌ {ticker}: Keine Daten verfügbar")
            self.skipped_tickers.append(ticker)
            return False

    def _download_ticker_data(self, ticker, start_date, end_date):
        """Lädt Daten für einen Ticker von Yahoo Finance herunter"""
        try:
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)

            # Leere Antwort
            if data is None or data.empty:
                print(f"  ℹ️ {ticker}: Kein Datenrahmen zurückgegeben")
                return None

            # Falls MultiIndex → 'Close' extrahieren
            if isinstance(data.columns, pd.MultiIndex):
                if 'Close' in data.columns.get_level_values(0):
                    data = data['Close']
                else:
                    print(f"  ℹ️ {ticker}: 'Close'-Spalte in MultiIndex fehlt")
                    return None
            else:
                if 'Close' not in data.columns:
                    print(f"  ℹ️ {ticker}: 'Close'-Spalte fehlt")
                    return None
                data = data['Close']

            # Wichtig: in Serie umwandeln, falls nötig
            data = data.squeeze()

            if data.dropna().empty:
                print(f"  ℹ️ {ticker}: 'Close'-Spalte enthält nur NaNs")
                return None

            # Sauberer Export-Frame
            df = pd.DataFrame({
                'date': data.index,
                'ticker': ticker,
                'close': data.values
            })

            return df

        except Exception as e:
            print(f"  ⚠️ {ticker}: Download-Fehler: {e}")
            return None

    def _update_ticker_info(self, ticker, actual_start_date=None, actual_end_date=None):
        """Aktualisiert Ticker-Profilinformationen in der Info-Datei"""
        try:
            stock_info = yf.Ticker(ticker).info

            # Relevante Felder extrahieren
            data = {
                'ticker': ticker,
                'sector': stock_info.get('sector', 'N/A'),
                'industry': stock_info.get('industry', 'N/A'),
                'currency': stock_info.get('currency', 'N/A'),
                'country': stock_info.get('country', 'N/A'),
                'exchange': stock_info.get('exchange', 'N/A'),
                'quote_type': stock_info.get('quoteType', 'N/A'),
                'market_cap': stock_info.get('marketCap', 'N/A'),
                'employees': stock_info.get('fullTimeEmployees', 'N/A'),
                'website': stock_info.get('website', 'N/A'),
                'actual_start_date': actual_start_date.strftime('%Y-%m-%d') if actual_start_date else 'N/A',
                'actual_end_date': actual_end_date.strftime('%Y-%m-%d') if actual_end_date else 'N/A',
                'last_update': dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # Info-Datei vorbereiten
            columns = list(data.keys())
            if os.path.exists(self.ticker_info_file):
                info_df = pd.read_csv(self.ticker_info_file)

                # Stelle sicher, dass alle erforderlichen Spalten existieren
                for col in columns:
                    if col not in info_df.columns:
                        info_df[col] = 'N/A'
            else:
                info_df = pd.DataFrame(columns=columns)

            # Alten Eintrag entfernen
            info_df = info_df[info_df['ticker'] != ticker]

            # Neuen Eintrag einfügen
            info_df = pd.concat([info_df, pd.DataFrame([data])], ignore_index=True)
            info_df.to_csv(self.ticker_info_file, index=False)

            date_info = ""
            if actual_start_date and actual_end_date:
                date_info = f" / Start: {data['actual_start_date']} / Ende: {data['actual_end_date']}"

            print(f"  ℹ️ {ticker}: Info aktualisiert – {data['sector']} / {data['industry']} / {data['currency']}{date_info}")
            return True

        except Exception as e:
            print(f"  ⚠️ {ticker}: Info-Aktualisierung fehlgeschlagen: {e}")
            return False

    def get_price_data(self, tickers, start_date, end_date):
        """
        Lädt die vorhandenen Preisdaten für die angegebenen Ticker und den Zeitraum

        Parameters:
        -----------
        tickers : list
            Liste der Ticker-Symbole
        start_date : str
            Startdatum (YYYY-MM-DD)
        end_date : str
            Enddatum (YYYY-MM-DD)

        Returns:
        --------
        dict
            Dictionary mit Ticker als Schlüssel und DataFrame als Wert
        """
        result = {}
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        print(f"\n=== Lade Preisdaten für {len(tickers)} Ticker ({start_date.date()} bis {end_date.date()}) ===")

        for ticker in tickers:
            ticker_file = os.path.join(self.price_data_folder, f"{ticker}.csv")
            if os.path.exists(ticker_file):
                try:
                    # CSV richtig einlesen
                    data = pd.read_csv(ticker_file, parse_dates=['date'])

                    # Nach Zeitraum filtern
                    data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
                    if not data.empty:
                        # Sicherstellen, dass das Datum als Index gesetzt ist für die Rückgabe
                        # (für Kompatibilität mit dem Rest des Codes)
                        data_indexed = data.set_index('date')
                        result[ticker] = data_indexed
                        print(f"  ✅ {ticker}: {len(data)} Datenpunkte geladen")
                    else:
                        print(f"  ⚠️ {ticker}: Keine Daten im angegebenen Zeitraum")
                except Exception as e:
                    print(f"  ❌ {ticker}: Fehler beim Laden: {e}")
            else:
                print(f"  ⚠️ {ticker}: Keine Datei gefunden")

        print(f"  📊 Insgesamt {len(result)} Ticker mit Daten geladen")
        return result


# Hauptprogramm
if __name__ == "__main__":
    # Basis-Verzeichnis für Daten
    base_folder = '/content/drive/MyDrive/Stocks/Data'

    # Stock Data Manager initialisieren
    data_manager = StockDataManager(base_folder=base_folder)

    # SCHRITT 1: Ticker für einen bestimmten Zeitraum definieren
    print("\n==== SCHRITT 1: TICKER FÜR BACKTEST-ZEITRAUM DEFINIEREN ====")

    # Beispiel 1: AAPL für Mai 2025
    data_manager.add_tickers_for_period(
        tickers=['SPY'],
        period_start_date='2025-05-01',
        period_end_date='2025-05-31',
        source_name='SPY'
    )

    # Beispiel 2: Mehrere Ticker für April 2025
    data_manager.add_tickers_for_period(
        tickers = ['AAPL', 'ABBV', 'AMZN', 'AVGO', 'BRK-B', 'COST', 'GOOGL', 'JNJ', 'JPM', 'LLY', 'MA', 'META', 'MSFT', 'NFLX', 'NVDA', 'PG', 'TSLA', 'UNH', 'V', 'XOM'],
        period_start_date='2025-04-01',
        period_end_date='2025-04-30',
        source_name='TW'
    )

    # SCHRITT 2: Daten für alle definierten Ticker aktualisieren
    print("\n==== SCHRITT 2: DATEN FÜR ALLE DEFINIERTEN TICKER AKTUALISIEREN ====")
    data_manager.update_ticker_data(history_start='1990-01-01')


    """
    # Beispieldaten anzeigen
    ticker_df = pd.read_csv(data_manager.ticker_periods_file)
    unique_tickers = ticker_df['ticker'].unique().tolist()

    price_data = data_manager.get_price_data(
        tickers=unique_tickers,
        start_date='2025-04-01',
        end_date='2025-04-30'
    )

    for ticker, df in price_data.items():
        print(f"\nDaten für {ticker}:")
        print(df.head(3))
    """