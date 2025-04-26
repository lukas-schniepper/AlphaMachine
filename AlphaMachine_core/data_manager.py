import os
import time
import datetime as dt
import yfinance as yf
import pandas as pd
from sqlmodel import SQLModel, Session, create_engine, select
from AlphaMachine_core.config import DATABASE_URL
from AlphaMachine_core.models import TickerPeriod, TickerInfo, PriceData
from AlphaMachine_core.db import get_session

# DB-Engine & Session
engine = create_engine(DATABASE_URL, echo=False)
SQLModel.metadata.create_all(engine)
session = Session(engine)


class StockDataManager:
    """
    Data Manager für Aktien-Backtests mit PostgreSQL (Supabase).
    Speichert Perioden, Info- und Preisdaten direkt in der Datenbank.
    """
    def __init__(self):
        self.skipped_tickers = []

    def add_tickers_for_period(self, tickers, period_start_date, period_end_date=None, source_name="manual"):
        """
        Fügt Ticker-Perioden in die SQL-Datenbank ein.
        """
        start = pd.to_datetime(period_start_date).date()
        end = (pd.to_datetime(period_end_date).date() if period_end_date else
               (pd.to_datetime(start) + pd.offsets.MonthEnd(1)).date())
        created = []
        for t in tickers:
            exists = session.exec(
                select(TickerPeriod).where(
                    TickerPeriod.ticker == t,
                    TickerPeriod.start_date == start,
                    TickerPeriod.end_date == end
                )
            ).first()
            if not exists:
                obj = TickerPeriod(
                    ticker=t,
                    start_date=start,
                    end_date=end,
                    source=source_name
                )
                session.add(obj)
                created.append(t)
        if created:
            session.commit()
        return created

    def update_ticker_data(self, tickers=None, history_start='1990-01-01'):
        """
        Lädt Preisdaten von Yahoo Finance und speichert sie in der DB-Tabelle PriceData.
        Führt Delta-Load durch, basierend auf dem letzten Datum in PriceData.
        """
        # Ticker-Liste ermitteln
        if tickers is None:
            tickers = [r.ticker for r in session.exec(select(TickerPeriod.ticker)).unique()]
        updated = []
        history_dt = pd.to_datetime(history_start).date()
        today = dt.date.today()

        for ticker in tickers:
            try:
                # Letztes Datum aus DB
                last = session.exec(
                    select(PriceData.date).where(PriceData.ticker == ticker).order_by(PriceData.date.desc())
                ).first()
                start_date = (last + dt.timedelta(days=1)) if last else history_dt
                if start_date >= today:
                    updated.append(ticker)
                    continue

                # Download
                df = yf.download(ticker, start=start_date, end=today + dt.timedelta(days=1), progress=False)
                if df is None or df.empty:
                    continue

                # Extract Close
                if isinstance(df.columns, pd.MultiIndex):
                    df = df['Close'] if 'Close' in df.columns.get_level_values(0) else df
                else:
                    df = df['Close'] if 'Close' in df.columns else df

                df = df.rename_axis('date').reset_index()[['date', 'Close']]
                df.columns = ['date', 'close']
                df['ticker'] = ticker

                # Insert or update each row
                for _, row in df.iterrows():
                    exists = session.exec(
                        select(PriceData).where(
                            PriceData.ticker == ticker,
                            PriceData.date == row['date']
                        )
                    ).first()
                    if not exists:
                        rec = PriceData(
                            ticker=row['ticker'],
                            date=row['date'],
                            close=row['close']
                        )
                        session.add(rec)
                session.commit()

                # Update TickerInfo
                self._update_ticker_info(ticker)
                updated.append(ticker)
                time.sleep(0.2)

            except Exception as e:
                print(f"Fehler bei {ticker}: {e}")
                self.skipped_tickers.append(ticker)
        return updated

    def _update_ticker_info(self, ticker):
        """
        Aktualisiert Metadaten zu einem Ticker in DB basierend auf PriceData.
        """
        try:
            info = yf.Ticker(ticker).info
            first_date = session.exec(
                select(PriceData.date).where(PriceData.ticker == ticker).order_by(PriceData.date)
            ).first()
            last_date = session.exec(
                select(PriceData.date).where(PriceData.ticker == ticker).order_by(PriceData.date.desc())
            ).first()
            data = {
                'ticker': ticker,
                'sector': info.get('sector', 'N/A'),
                'currency': info.get('currency', 'N/A'),
                'actual_start_date': first_date,
                'actual_end_date': last_date,
                'last_update': dt.datetime.now().date()
            }
            obj = session.exec(select(TickerInfo).where(TickerInfo.ticker == ticker)).first()
            if obj:
                for k,v in data.items(): setattr(obj, k, v)
            else:
                session.add(TickerInfo(**data))
            session.commit()
            return True
        except Exception as e:
            print(f"Info-Update fehlgeschlagen für {ticker}: {e}")
            return False

    def get_periods(self, month: str, source: str):
        """
        Liefert TickerPeriod-Einträge für Monat (YYYY-MM) und Quelle.
        """
        return session.exec(
            select(TickerPeriod).where(
                TickerPeriod.start_date.startswith(month),
                TickerPeriod.source == source
            )
        ).all()

    def get_ticker_info(self):
        """
        Liefert alle TickerInfo-Einträge.
        """
        return session.exec(select(TickerInfo)).all()

    def get_price_data(self, tickers, start_date, end_date):
        """
        Liefert PriceData-Records aus DB für die Ticker und den Zeitraum.
        """
        sd = pd.to_datetime(start_date)
        ed = pd.to_datetime(end_date)
        return session.exec(
            select(PriceData).where(
                PriceData.ticker.in_(tickers),
                PriceData.date >= sd,
                PriceData.date <= ed
            )
        ).all()
