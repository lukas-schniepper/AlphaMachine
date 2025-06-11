# AlphaMachine_core/data_manager.py
import time
import datetime as dt
import yfinance as yf 
import pandas as pd
from typing import Dict, Optional, List, Any
from sqlmodel import select
from sqlalchemy import func 

from AlphaMachine_core.models import TickerPeriod, TickerInfo, PriceData
from AlphaMachine_core.db import get_session

class StockDataManager:
    def __init__(self):
        self.skipped_tickers: List[str] = []

    def add_tickers_for_period(self, tickers: List[str], period_start_date: str, period_end_date: Optional[str] = None, source_name: str = "manual") -> List[str]:
        start = pd.to_datetime(period_start_date).date()
        end = (pd.to_datetime(period_end_date).date() if period_end_date
            else (pd.to_datetime(start) + pd.offsets.MonthEnd(0)).date())
        created_tickers: List[str] = []
        with get_session() as session:
            for t_str in tickers:
                t = t_str.upper() 
                statement = select(TickerPeriod).where(
                    TickerPeriod.ticker    == t,
                    TickerPeriod.start_date == start,
                    TickerPeriod.end_date   == end,
                    TickerPeriod.source     == source_name
                )
                exists = session.exec(statement).first()
                if not exists:
                    obj = TickerPeriod(ticker=t, start_date=start, end_date=end, source=source_name)
                    session.add(obj)
                    created_tickers.append(t)
            if created_tickers:
                session.commit()
        return created_tickers

    def update_ticker_data(self, tickers: Optional[List[str]] = None, history_start: str = '1990-01-01') -> List[str]:
        target_tickers_list: List[str]
        if tickers is None:
            with get_session() as session:
                results = session.exec(select(TickerPeriod.ticker).distinct()).all()
                target_tickers_list = sorted([str(ticker_val) for ticker_val in results if ticker_val])
        else:
            target_tickers_list = [t.upper() for t in tickers]

        history_dt = pd.to_datetime(history_start).date()
        today = dt.date.today()
        updated_tickers_list: List[str] = []

        for ticker_str_upper in target_tickers_list:
            last_date_in_db: Optional[dt.date] = None
            with get_session() as session:
                result = session.exec(
                    select(PriceData.trade_date)
                    .where(PriceData.ticker == ticker_str_upper)
                    .order_by(PriceData.trade_date.desc())
                ).first()
                if result:
                    last_date_in_db = result 
            
            start_date_for_yf = (last_date_in_db + dt.timedelta(days=1)) if last_date_in_db else history_dt
            
            if start_date_for_yf > today:
                continue

            print(f"Lade yf-Daten für {ticker_str_upper} von {start_date_for_yf} bis {today + dt.timedelta(days=1)}")
            try:
                raw = yf.download(
                    ticker_str_upper, start=start_date_for_yf, end=today + dt.timedelta(days=1),
                    progress=False, auto_adjust=False, timeout=10
                )
            except Exception as e_yf:
                print(f"Fehler bei yf.download für {ticker_str_upper}: {e_yf}")
                self.skipped_tickers.append(ticker_str_upper); continue

            if raw.empty:
                print(f"Keine neuen Daten von yfinance für {ticker_str_upper} seit {start_date_for_yf} gefunden.")
                self.skipped_tickers.append(ticker_str_upper); continue
            
            if isinstance(raw.columns, pd.MultiIndex):
                if ticker_str_upper in raw.columns.get_level_values(1):
                    raw = raw.xs(ticker_str_upper, axis=1, level=1)
                else: 
                    raw.columns = raw.columns.droplevel(0) # Annahme: oberstes Level kann weg, wenn Ticker nicht im 2. ist

            expected_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in raw.columns for col in expected_cols):
                print(f"WARNUNG: Fehlende Spalten für {ticker_str_upper}: {raw.columns.tolist()}. Überspringe.")
                self.skipped_tickers.append(ticker_str_upper); continue
            
            df = raw[expected_cols].copy()
            df.dropna(subset=['Close', 'Volume'], inplace=True)
            df = df[df['Volume'] > 0] 
            if df.empty: continue

            df.reset_index(inplace=True)
            date_col_name = None
            if 'Date' in df.columns: date_col_name = 'Date'
            elif 'Datetime' in df.columns: date_col_name = 'Datetime' # yfinance gibt manchmal 'Datetime' zurück
            if not date_col_name: self.skipped_tickers.append(ticker_str_upper); continue
            
            df.rename(columns={date_col_name: 'trade_date_dt_col'}, inplace=True)
            df['ticker'] = ticker_str_upper
            df['trade_date'] = pd.to_datetime(df['trade_date_dt_col']).dt.date

            new_df = df 
            if new_df.empty: continue

            with get_session() as session:
                print(f"Füge {len(new_df)} neue Preisdatensätze für {ticker_str_upper} zur DB hinzu...")
                objs_to_add = []
                for _, r in new_df.iterrows():
                    try:
                        if pd.isna(r['Open']) or pd.isna(r['High']) or pd.isna(r['Low']) or pd.isna(r['Close']) or pd.isna(r['Volume']):
                            continue
                        objs_to_add.append(PriceData(
                            ticker=str(r['ticker']), trade_date=r['trade_date'], 
                            open=float(r['Open']), high=float(r['High']),
                            low=float(r['Low']), close=float(r['Close']),
                            volume=int(r['Volume'])
                        ))
                    except Exception as e_pd_conv:
                        print(f"Fehler Konvertierung PriceData Record ({ticker_str_upper}, Datum {r.get('trade_date')}): {e_pd_conv}")
                
                if objs_to_add: session.add_all(objs_to_add); session.commit()
                else: print(f"Keine validen Objekte für {ticker_str_upper} zum Hinzufügen.")
            
            self._update_ticker_info(ticker_str_upper)
            updated_tickers_list.append(ticker_str_upper)
            time.sleep(0.25) # Etwas längere Pause
        return updated_tickers_list

    def _update_ticker_info(self, ticker: str) -> bool:
        print(f"Versuche Ticker-Info für {ticker} zu aktualisieren...")
        try:
            yf_ticker_obj = yf.Ticker(ticker)
            info = yf_ticker_obj.info # API-Aufruf (kann leer sein oder Fehler werfen)

            if not info: # Manchmal gibt yfinance ein leeres Dict zurück
                print(f"⚠️ Keine Info von yfinance für {ticker} erhalten.")
                return False
            
            data_to_update: Dict[str, Any] = {
                'ticker': ticker,
                'sector': str(info.get('sector', 'N/A'))[:255] if info.get('sector') else None,
                'industry': str(info.get('industry', 'N/A'))[:255] if info.get('industry') else None,
                'currency': str(info.get('currency', 'N/A'))[:10] if info.get('currency') else None,
                'country': str(info.get('country', 'N/A'))[:255] if info.get('country') else None,
                'exchange': str(info.get('exchange', 'N/A'))[:50] if info.get('exchange') else None,
                'quote_type': str(info.get('quoteType', 'N/A'))[:50] if info.get('quoteType') else None,
                # Sicherer Zugriff auf potenziell fehlende Keys
                'market_cap': float(info.get('marketCap')) if info.get('marketCap') is not None else None,
                'employees': int(info.get('fullTimeEmployees')) if info.get('fullTimeEmployees') is not None else None,
                'website': str(info.get('website', 'N/A'))[:255] if info.get('website') else None,
                'last_update': dt.date.today()
            }

            with get_session() as session:
                date_range_statement = select(func.min(PriceData.trade_date), func.max(PriceData.trade_date)).where(PriceData.ticker == ticker)
                date_range_result = session.exec(date_range_statement).first()
                
                if date_range_result and date_range_result[0] is not None:
                    data_to_update['actual_start_date'] = date_range_result[0]
                    data_to_update['actual_end_date'] = date_range_result[1]
                else: 
                    data_to_update['actual_start_date'] = None
                    data_to_update['actual_end_date'] = None
                
                db_ticker_info = session.exec(select(TickerInfo).where(TickerInfo.ticker == ticker)).first()
                if db_ticker_info:
                    for key, value in data_to_update.items(): setattr(db_ticker_info, key, value)
                    print(f"TickerInfo für {ticker} aktualisiert.")
                else:
                    db_ticker_info = TickerInfo(**data_to_update)
                    session.add(db_ticker_info)
                    print(f"TickerInfo für {ticker} neu erstellt.")
                session.commit()
            return True
        except Exception as e: # Breiterer Exception-Fang für yfinance-Info-Probleme
            print(f"⚠️ Fehler _update_ticker_info für {ticker}: {e}")
            return False

    def get_periods(self, month: str, source: str) -> List[Dict[str, Any]]:
        period_dicts = []
        with get_session() as session:
            statement = select(TickerPeriod).where(
                func.to_char(TickerPeriod.start_date, 'YYYY-MM') == month,
                TickerPeriod.source == source
            )
            results = session.exec(statement).all()
            for db_obj in results: period_dicts.append(db_obj.model_dump()) 
        return period_dicts

    def get_ticker_info(self) -> List[Dict[str, Any]]:
        info_dicts = []
        with get_session() as session:
            results = session.exec(select(TickerInfo)).all()
            for db_obj in results: info_dicts.append(db_obj.model_dump())
        return info_dicts
    
    def get_price_data(self, tickers: List[str], start_date: Optional[dt.date], end_date: Optional[dt.date]) -> List[Dict[str, Any]]:
        sd = pd.to_datetime(start_date).date() if start_date else None
        ed = pd.to_datetime(end_date).date() if end_date else None
        price_data_dicts = []
        with get_session() as session:
            if not tickers: return []
            upper_tickers = [t.upper() for t in tickers]
            statement = select(PriceData).where(PriceData.ticker.in_(upper_tickers))
            if sd: statement = statement.where(PriceData.trade_date >= sd)
            if ed: statement = statement.where(PriceData.trade_date <= ed)
            statement = statement.order_by(PriceData.ticker, PriceData.trade_date)
            results = session.exec(statement).all()
            for record in results: price_data_dicts.append(record.model_dump())
        if not price_data_dicts and tickers: print(f"SDM.get_price_data: Keine Daten für {tickers} im Zeitraum {sd} bis {ed}")
        return price_data_dicts

    def delete_period(self, period_id: int) -> bool:
        with get_session() as session:
            obj = session.get(TickerPeriod, period_id) # get() erwartet den Primärschlüssel
            if obj: session.delete(obj); session.commit(); return True
        return False

    def get_periods_distinct_months(self) -> List[str]:
        with get_session() as session:
            results = session.exec(select(func.to_char(TickerPeriod.start_date, 'YYYY-MM')).distinct()).all()
        return [str(row) for row in results if row is not None]

    def get_tickers_for(self, month: str, sources: List[str]) -> List[str]:
        with get_session() as session:
            results = session.exec(
                select(TickerPeriod.ticker).distinct()
                .where(
                    func.to_char(TickerPeriod.start_date, 'YYYY-MM') == month,
                    TickerPeriod.source.in_(sources)
                )
            ).all()
        return [str(row) for row in results if row is not None]