import os
import time
import datetime as dt
import yfinance as yf
import pandas as pd
from sqlmodel import select
from AlphaMachine_core.models import TickerPeriod, TickerInfo, PriceData
from AlphaMachine_core.db import get_session
from sqlalchemy import func

class StockDataManager:
    """
    Data Manager für Aktien-Backtests mit PostgreSQL (Supabase).
    Speichert Perioden, Info- und Preisdaten direkt in der Datenbank.
    """
    def __init__(self):
        self.skipped_tickers = []

    def add_tickers_for_period(self, tickers, period_start_date, period_end_date=None, source_name="manual"):
        start = pd.to_datetime(period_start_date).date()
        end = (pd.to_datetime(period_end_date).date() if period_end_date else
               (pd.to_datetime(start) + pd.offsets.MonthEnd(1)).date())
        created = []
        with get_session() as session:
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
        if tickers is None:
            with get_session() as session:
                tickers = session.exec(select(TickerPeriod.ticker)).all()

        history_dt = pd.to_datetime(history_start).date()
        today = dt.date.today()
        updated = []

        for ticker in tickers:
            with get_session() as session:
                last = session.exec(
                    select(PriceData.date)
                    .where(PriceData.ticker == ticker)
                    .order_by(PriceData.date.desc())
                ).first()
            start_date = (last + dt.timedelta(days=1)) if last else history_dt

            raw = yf.download(
                ticker,
                start=start_date,
                end=today + dt.timedelta(days=1),
                progress=False,
                auto_adjust=False
            )
            if raw.empty:
                continue
            if isinstance(raw.columns, pd.MultiIndex):
                raw = raw.xs(ticker, axis=1, level=1)

            df = raw[['Open', 'High', 'Low', 'Close', 'Volume']].reset_index()
            df.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }, inplace=True)
            df['ticker'] = ticker
            df['date'] = pd.to_datetime(df['date']).dt.date

            if last:
                new_df = df[df['date'] > last]
            else:
                new_df = df
            if new_df.empty:
                continue

            with get_session() as session:
                objs = [
                    PriceData(
                        ticker=r['ticker'],
                        date=r['date'],
                        open=float(r['open']),
                        high=float(r['high']),
                        low=float(r['low']),
                        close=float(r['close']),
                        volume=int(r['volume'])
                    ) for r in new_df.to_dict('records')
                ]
                session.add_all(objs)
                session.commit()

            self._update_ticker_info(ticker)
            updated.append(ticker)
            time.sleep(0.2)

        return updated

    def _update_ticker_info(self, ticker: str) -> bool:
        try:
            info = yf.Ticker(ticker).info
            with get_session() as session:
                first_date = session.exec(
                    select(PriceData.date)
                    .where(PriceData.ticker == ticker)
                    .order_by(PriceData.date)
                ).first()
                last_date = session.exec(
                    select(PriceData.date)
                    .where(PriceData.ticker == ticker)
                    .order_by(PriceData.date.desc())
                ).first()

                if isinstance(first_date, tuple):
                    first_date = first_date[0]
                if isinstance(last_date, tuple):
                    last_date = last_date[0]

                data = {
                    'ticker': ticker,
                    'sector': info.get('sector', None),
                    'industry': info.get('industry', None),
                    'currency': info.get('currency', None),
                    'country': info.get('country', None),
                    'exchange': info.get('exchange', None),
                    'quote_type': info.get('quoteType', None),
                    'market_cap': info.get('marketCap', None),
                    'employees': info.get('fullTimeEmployees', None),
                    'website': info.get('website', None),
                    'actual_start_date': first_date,
                    'actual_end_date': last_date,
                    'last_update': dt.date.today()
                }

                obj = session.exec(
                    select(TickerInfo).where(TickerInfo.ticker == ticker)
                ).first()

                if obj:
                    for k, v in data.items():
                        setattr(obj, k, v)
                else:
                    session.add(TickerInfo(**data))

                session.commit()

            return True
        except Exception as e:
            print(f"⚠️ _update_ticker_info für {ticker} fehlgeschlagen: {e}")
            return False

    def get_periods(self, month: str, source: str):
        with get_session() as session:
            return session.exec(
                select(TickerPeriod)
                .where(
                    func.to_char(TickerPeriod.start_date, 'YYYY-MM') == month,
                    TickerPeriod.source == source
                )
            ).all()

    def get_ticker_info(self):
        with get_session() as session:
            return session.exec(
                select(TickerInfo)
            ).all()

    def get_price_data(self, tickers, start_date, end_date):
        sd = pd.to_datetime(start_date)
        ed = pd.to_datetime(end_date)
        with get_session() as session:
            return session.exec(
                select(PriceData)
                .where(
                    PriceData.ticker.in_(tickers),
                    PriceData.date >= sd,
                    PriceData.date <= ed
                )
            ).all()

    def delete_period(self, period_id: int) -> bool:
        with get_session() as session:
            obj = session.get(TickerPeriod, period_id)
            if obj:
                session.delete(obj)
                session.commit()
                return True
        return False
