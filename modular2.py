import streamlit as st
import sqlite3
import pandas as pd
import yfinance as yf
import asyncio
import telegram
import time
import schedule
import threading
import uuid
from datetime import datetime, date, timedelta
import pytz
import logging
import io
import csv
import json
import requests
from bs4 import BeautifulSoup
import os
import pyperclip
import difflib
import re
import nest_asyncio

# Apply nest_asyncio to handle nested event loops in Streamlit
nest_asyncio.apply()

# Configure logging
logging.basicConfig(
   filename='stock_screener_alerts.log',
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize session state keys
def initialize_session_state():
   defaults = {
       'quarterly_data': {},
       'yearly_data': {},
       'finance_override': False,
       'enable_same_quarter': False,
       'force_refresh': False,
       'scheduler_thread': True,
       'strategies': None
   }
   for key, value in defaults.items():
       if key not in st.session_state:
           st.session_state[key] = value

# Database operations
class DatabaseManager:
   def __init__(self, db_name='stock_alerts_modular.db'):
       self.db_name = db_name
       self.init_db()

   def init_db(self):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute('''CREATE TABLE IF NOT EXISTS stocks
                        (id TEXT PRIMARY KEY,
                         symbol TEXT,
                         initial_price REAL,
                         alert_price REAL,
                         target_price REAL,
                         strategy TEXT,
                         enabled INTEGER,
                         created_at INTEGER,
                         alert_triggered INTEGER DEFAULT 0,
                         last_notified_alert INTEGER DEFAULT 0,
                         last_notified_target INTEGER DEFAULT 0,
                         notification_cooldown INTEGER DEFAULT 600)''')
           c.execute('''CREATE TABLE IF NOT EXISTS notifications
                        (id TEXT PRIMARY KEY,
                         symbol TEXT,
                         strategy TEXT,
                         notification_type TEXT,
                         price REAL,
                         timestamp INTEGER)''')
           c.execute("PRAGMA table_info(stocks)")
           columns = [info[1] for info in c.fetchall()]
           for column, column_type, default in [
               ('initial_price', 'REAL', '0'),
               ('created_at', 'INTEGER', '0'),
               ('alert_triggered', 'INTEGER', '0'),
               ('last_notified_alert', 'INTEGER', '0'),
               ('last_notified_target', 'INTEGER', '0'),
               ('notification_cooldown', 'INTEGER', '600')
           ]:
               if column not in columns:
                   c.execute(f"ALTER TABLE stocks ADD COLUMN {column} {column_type} DEFAULT {default}")
           c.execute('''CREATE TABLE IF NOT EXISTS strategies
                        (id TEXT PRIMARY KEY, name TEXT)''')
           c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_strategy ON stocks (symbol, strategy)''')
           conn.commit()

   def add_stock(self, symbol, strategy, initial_price, alert_price, target_price, default_cooldown):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("SELECT id FROM stocks WHERE symbol = ? AND strategy = ?", (symbol.upper(), strategy))
           if c.fetchone():
               logging.info(f"Stock {symbol.upper()} with strategy {strategy} already exists in alerts. Skipping auto-add.")
               return False
           try:
               created_at = int(time.time())
               c.execute("""INSERT INTO stocks 
                            (id, symbol, initial_price, alert_price, target_price, strategy, enabled, created_at, 
                            alert_triggered, last_notified_alert, last_notified_target, notification_cooldown) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (str(uuid.uuid4()), symbol.upper(), initial_price, alert_price, target_price, strategy, 1, 
                         created_at, 0, 0, 0, default_cooldown))
               conn.commit()
               logging.info(f"Auto-added stock {symbol.upper()} to alerts with strategy {strategy}, initial price ₹{initial_price:.2f}, alert price ₹{alert_price:.2f}, target price ₹{target_price:.2f}")
               return True
           except Exception as e:
               logging.error(f"Error auto-adding stock {symbol.upper()} to alerts: {e}")
               return False

   def add_notification(self, symbol, strategy, notification_type, price, timestamp):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("""INSERT INTO notifications 
                        (id, symbol, strategy, notification_type, price, timestamp) 
                        VALUES (?, ?, ?, ?, ?, ?)""",
                    (str(uuid.uuid4()), symbol.upper(), strategy, notification_type, price, timestamp))
           conn.commit()
           logging.info(f"Stored {notification_type} notification for {symbol.upper()} with strategy {strategy} at price ₹{price:.2f}")

   def get_notifications(self):
       with sqlite3.connect(self.db_name) as conn:
           return pd.read_sql_query("SELECT * FROM notifications", conn)

   def clear_notifications(self):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("DELETE FROM notifications")
           conn.commit()
           logging.info("Cleared all notifications from database")

   def get_strategies(self):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("SELECT name FROM strategies")
           strategies = [row[0] for row in c.fetchall()]
           if not strategies:
               strategies = ["SMA", "V20", "52WLowHigh"]
               for strategy in strategies:
                   c.execute("INSERT INTO strategies (id, name) VALUES (?, ?)", (str(uuid.uuid4()), strategy))
               conn.commit()
           return strategies

   def add_strategy(self, strategy_name):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("INSERT INTO strategies (id, name) VALUES (?, ?)", (str(uuid.uuid4()), strategy_name))
           conn.commit()
           logging.info(f"Added strategy: {strategy_name}")

   def get_enabled_stocks(self):
       with sqlite3.connect(self.db_name) as conn:
           return pd.read_sql_query("SELECT * FROM stocks WHERE enabled = 1", conn)

   def delete_stock(self, stock_id):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("DELETE FROM stocks WHERE id = ?", (stock_id,))
           conn.commit()
           logging.info(f"Deleted stock with id {stock_id}")

   def toggle_stock_status(self, stock_id, current_status):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           new_status = 0 if current_status else 1
           c.execute("UPDATE stocks SET enabled = ? WHERE id = ?", (new_status, stock_id))
           conn.commit()
           logging.info(f"{'Enabled' if new_status else 'Disabled'} stock with id {stock_id}")
           return new_status

   def update_stock_prices(self, stock_id, alert_price, target_price):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("UPDATE stocks SET alert_price = ?, target_price = ? WHERE id = ?",
                    (alert_price, target_price, stock_id))
           conn.commit()
           logging.info(f"Updated stock with id {stock_id}: alert price ₹{alert_price:.2f}, target price ₹{target_price:.2f}")

   def reset_alerts(self):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("UPDATE stocks SET alert_triggered = 0, last_notified_alert = 0")
           conn.commit()
           logging.info("Reset alert triggered for all stocks")

   def delete_all_stocks(self):
       with sqlite3.connect(self.db_name) as conn:
           c = conn.cursor()
           c.execute("DELETE FROM stocks")
           conn.commit()
           logging.info("Deleted all stocks from database")

   def get_all_stocks(self):
       with sqlite3.connect(self.db_name) as conn:
           return pd.read_sql_query("SELECT * FROM stocks", conn)

# Data fetching module
class DataFetcher:
   @staticmethod
   @st.cache_data
   def scrape_screener_data(ticker):
       headers = {
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
       }
       urls = [
           f"https://www.screener.in/company/{ticker}/consolidated/",
           f"https://www.screener.in/company/{ticker}/"
       ]
       quarterly_data = None
       yearly_data = None
       error = None
       for url in urls:
           try:
               response = requests.get(url, headers=headers, timeout=10)
               response.raise_for_status()
               soup = BeautifulSoup(response.content, 'html.parser')
               tables = soup.find_all('table', class_='data-table')
               data_found = False
               for table in tables:
                   tbody = table.find('tbody')
                   if tbody and any(tr.find_all('td') for tr in tbody.find_all('tr')):
                       section = table.find_parent('section')
                       if section:
                           if section.get('id') == 'quarters':
                               quarterly_data = FinancialAnalyzer.parse_table(table)
                               if (quarterly_data is not None and not quarterly_data.empty and
                                   quarterly_data.shape[1] >= 2 and quarterly_data.shape[0] >= 1 and
                                   str(quarterly_data.iloc[:, 1].values[0]).strip() != ""):
                                   data_found = True
                           elif section.get('id') == 'profit-loss':
                               yearly_data = FinancialAnalyzer.parse_table(table)
                               if yearly_data is not None and not yearly_data.empty and yearly_data.shape[0] >= 1:
                                   data_found = True
               if data_found:
                   break
           except requests.RequestException as e:
               error = f"Error fetching {url} for ticker {ticker}: {e}"
           if url == urls[1] and quarterly_data is None and yearly_data is None:
               error = f"No financial data tables with valid data found for ticker {ticker}"
       return quarterly_data, yearly_data, error

   @staticmethod
   def get_stock_data(symbol, period="1y"):
       try:
           ticker = symbol.upper() if symbol.endswith('.NS') else f"{symbol.upper()}.NS"
           stock = yf.Ticker(ticker)
           info = stock.info
           current_price = info.get('regularMarketPrice', None)
           if current_price is None:
               logging.warning(f"No valid current price for {symbol}. Response: {info}")
               return None, None, None
           hist = stock.history(period=period, interval="1d")
           if hist.empty:
               logging.warning(f"No historical data for {symbol}")
               return current_price, None, None
           v20_range = None
           for i in range(len(hist) - 1, -1, -1):
               current_candle = hist.iloc[i]
               if current_candle['Close'] > current_candle['Open']:
                   for j in range(i - 1, -1, -1):
                       prev_candle = hist.iloc[j]
                       gain_percent = ((current_candle['High'] - prev_candle['Low']) / prev_candle['Low']) * 100
                       if gain_percent >= 20:
                           momentum_broken = False
                           for k in range(j + 1, i + 1):
                               if hist.iloc[k]['Close'] < hist.iloc[k]['Open']:
                                   momentum_broken = True
                                   break
                           if not momentum_broken:
                               v20_range = (prev_candle['Low'], current_candle['High'])
                               break
                   if v20_range:
                       break
           week_52_low = hist['Low'].min()
           week_52_high = hist['High'].max()
           logging.info(f"Fetched data for {symbol}: Current price ₹{current_price:.2f}, V20 range {v20_range}, 52W Low ₹{week_52_low:.2f}, 52W High ₹{week_52_high:.2f}")
           return current_price, v20_range, (week_52_low, week_52_high)
       except json.JSONDecodeError as e:
           logging.error(f"JSON decode error for {symbol}: {e}")
           return None, None, None
       except Exception as e:
           logging.error(f"Error fetching data for {symbol}: {e}")
           return None, None, None

# Financial calculations module
class FinancialAnalyzer:
   @staticmethod
   def parse_table(table):
       headers = [th.text.strip() for th in table.find('thead').find_all('th')]
       if not headers or len(headers) < 2:
           return None
       headers[0] = ''
       rows = []
       for tr in table.find('tbody').find_all('tr'):
           cells = [td.text.strip() for td in tr.find_all('td')]
           if cells and len(cells) == len(headers):
               rows.append(cells)
       return pd.DataFrame(rows, columns=headers) if rows else None

   @staticmethod
   def load_from_csv(ticker):
       output_dir = 'output_tables'
       quarterly_file = os.path.join(output_dir, f'{ticker}_quarterly_results.csv')
       yearly_file = os.path.join(output_dir, f'{ticker}_profit_loss.csv')
       quarterly_data = None
       yearly_data = None
       error = None
       try:
           if os.path.exists(quarterly_file):
               quarterly_data = pd.read_csv(quarterly_file)
               if quarterly_data.empty or quarterly_data.shape[1] < 2:
                   quarterly_data = None
                   error = f"Invalid or empty quarterly CSV data for {ticker}"
               else:
                   if quarterly_data.columns[0] != '':
                       quarterly_data.columns = [''] + quarterly_data.columns[1:].tolist()
           else:
               error = f"Quarterly CSV file not found for {ticker}"
       except Exception as e:
           error = f"Error loading quarterly CSV for {ticker}: {e}"
       try:
           if os.path.exists(yearly_file):
               yearly_data = pd.read_csv(yearly_file)
               if yearly_data.empty or yearly_data.shape[1] < 2:
                   yearly_data = None
                   error = f"Invalid or empty yearly CSV data for {ticker}"
               else:
                   if yearly_data.columns[0] != '':
                       yearly_data.columns = [''] + yearly_data.columns[1:].tolist()
       except Exception as e:
           error = f"Error loading yearly CSV for {ticker}: {e}" if not error else error
       return quarterly_data, yearly_data, error

   @staticmethod
   def save_to_csv(quarterly_data, yearly_data, ticker):
       output_dir = 'output_tables'
       os.makedirs(output_dir, exist_ok=True)
       if quarterly_data is not None and not quarterly_data.empty:
           quarterly_data.to_csv(os.path.join(output_dir, f'{ticker}_quarterly_results.csv'), index=False)
       if yearly_data is not None and not yearly_data.empty:
           yearly_data.to_csv(os.path.join(output_dir, f'{ticker}_profit_loss.csv'), index=False)

   @staticmethod
   def is_finance_company(quarterly_data):
       if quarterly_data is None or quarterly_data.empty:
           return False
       return "Financing Profit" in quarterly_data.iloc[:, 0].values

   @staticmethod
   def find_row(data, row_name, threshold=0.8):
       possible_names = [row_name, row_name.replace(" ", ""), "Consolidated " + row_name, row_name + " (Consolidated)"]
       for name in possible_names:
           for index in data.index:
               if name.lower() in index.lower():
                   return index
       matches = difflib.get_close_matches(row_name.lower(), [idx.lower() for idx in data.index], n=1, cutoff=threshold)
       return matches[0] if matches else None

   @staticmethod
   def clean_numeric(series):
       if isinstance(series, pd.DataFrame):
           series = series.iloc[0]
       elif not isinstance(series, pd.Series):
           series = pd.Series(series)
       series = series.astype(str).str.replace(',', '', regex=False).str.replace('[^0-9.-]', '', regex=True)
       return pd.to_numeric(series, errors='coerce').fillna(0)

   @staticmethod
   def adjust_non_finance(data, is_finance):
       if is_finance:
           net_profit_row = FinancialAnalyzer.find_row(data, "Net Profit") or FinancialAnalyzer.find_row(data, "Profit after tax")
           actual_income_row = FinancialAnalyzer.find_row(data, "Actual Income") or net_profit_row
           net_profit = FinancialAnalyzer.clean_numeric(data.loc[net_profit_row].iloc[1:]) if net_profit_row and net_profit_row in data.index else None
           actual_income = FinancialAnalyzer.clean_numeric(data.loc[actual_income_row].iloc[1:]) if actual_income_row and actual_income_row in data.index else None
           return net_profit, actual_income
       net_profit_row = FinancialAnalyzer.find_row(data, "Net Profit") or FinancialAnalyzer.find_row(data, "Profit after tax")
       actual_income_row = FinancialAnalyzer.find_row(data, "Actual Income") or net_profit_row
       other_income_row = FinancialAnalyzer.find_row(data, "Other Income")
       net_profit = FinancialAnalyzer.clean_numeric(data.loc[net_profit_row].iloc[1:]) if net_profit_row and net_profit_row in data.index else None
       actual_income = FinancialAnalyzer.clean_numeric(data.loc[actual_income_row].iloc[1:]) if actual_income_row and actual_income_row in data.index else net_profit
       other_income = FinancialAnalyzer.clean_numeric(data.loc[other_income_row].iloc[1:]) if other_income_row and other_income_row in data.index else pd.Series(0, index=net_profit.index if net_profit is not None else [])
       adjusted_net_profit = net_profit - other_income if net_profit is not None else None
       adjusted_actual_income = actual_income - other_income if actual_income is not None else adjusted_net_profit
       return adjusted_net_profit, adjusted_actual_income

   
   @staticmethod
   def is_ascending(series):
        if series is None or series.empty:
            return False
        return all(series.iloc[i] <= series.iloc[i + 1] for i in range(len(series) - 1))

   @staticmethod
   def extract_quarter_year(column):
       patterns = [
           r'(\w+)\s+(\d{4})', r'(\w+)-(\d{2})', r'(\w+)\s*\'(\d{2})'
       ]
       column = column.strip()
       for pattern in patterns:
           match = re.match(pattern, column)
           if match:
               quarter, year = match.groups()
               year = int(year) if len(year) == 4 else int("20" + year)
               return quarter, year
       return None, None

   @staticmethod
   def check_same_quarter_comparison(data, enable_same_quarter):
       if not enable_same_quarter or data is None or data.empty or '' not in data.columns:
           return {
               'Same Quarter Net Profit (Adjusted)': 'N/A',
               'Same Quarter Net Profit (Raw)': 'N/A'
           }
       data = data.set_index('')
       adjusted_net_profit, _ = FinancialAnalyzer.adjust_non_finance(data, FinancialAnalyzer.is_finance_company(data))
       raw_net_profit = FinancialAnalyzer.clean_numeric(data.loc[FinancialAnalyzer.find_row(data, "Net Profit") or FinancialAnalyzer.find_row(data, "Profit after tax")].iloc[1:]) if FinancialAnalyzer.find_row(data, "Net Profit") or FinancialAnalyzer.find_row(data, "Profit after tax") else None
       results = {
           'Same Quarter Net Profit (Adjusted)': 'N/A',
           'Same Quarter Net Profit (Raw)': 'N/A'
       }
       if adjusted_net_profit is None or raw_net_profit is None:
           return results
       try:
           latest_column = data.columns[-1]
           latest_quarter, latest_year = FinancialAnalyzer.extract_quarter_year(latest_column)
           if latest_quarter is None or latest_year is None:
               return results
           prev_year_column = None
           for col in data.columns[:-1]:
               quarter, year = FinancialAnalyzer.extract_quarter_year(col)
               if quarter == latest_quarter and year == latest_year - 1:
                   prev_year_column = col
                   break
           if prev_year_column:
               latest_adj_np = adjusted_net_profit[latest_column]
               prev_adj_np = adjusted_net_profit[prev_year_column]
               latest_raw_np = raw_net_profit[latest_column]
               prev_raw_np = raw_net_profit[prev_year_column]
               results['Same Quarter Net Profit (Adjusted)'] = 'PASS' if latest_adj_np >= prev_adj_np else 'FAIL'
               results['Same Quarter Net Profit (Raw)'] = 'PASS' if latest_raw_np >= prev_raw_np else 'FAIL'
       except Exception:
           pass
       return results

   @staticmethod
   def check_highest_historical(data, is_quarterly, is_finance):
       if data is None or data.empty or '' not in data.columns:
           return {
               'Net Profit (Adjusted)': 'N/A', 'Actual Income (Adjusted)': 'N/A',
               'Raw Net Profit (Raw)': 'N/A', 'Raw Actual Income (Raw)': 'N/A',
               'Net Profit (Adjusted) Ascending': 'N/A', 'Actual Income (Adjusted) Ascending': 'N/A',
               'Raw Net Profit (Raw) Ascending': 'N/A', 'Raw Actual Income (Raw) Ascending': 'N/A'
           }
       data = data.set_index('')
       adjusted_net_profit, adjusted_actual_income = FinancialAnalyzer.adjust_non_finance(data, is_finance)
       raw_net_profit = FinancialAnalyzer.clean_numeric(data.loc[FinancialAnalyzer.find_row(data, "Net Profit") or FinancialAnalyzer.find_row(data, "Profit after tax")].iloc[1:]) if FinancialAnalyzer.find_row(data, "Net Profit") or FinancialAnalyzer.find_row(data, "Profit after tax") else None
       raw_actual_income = FinancialAnalyzer.clean_numeric(data.loc[FinancialAnalyzer.find_row(data, "Actual Income") or FinancialAnalyzer.find_row(data, "Net Profit") or FinancialAnalyzer.find_row(data, "Profit after tax")].iloc[1:]) if FinancialAnalyzer.find_row(data, "Actual Income") or FinancialAnalyzer.find_row(data, "Net Profit") or FinancialAnalyzer.find_row(data, "Profit after tax") else None
       results = {}
       for metric, values, prefix in [
           ("Net Profit (Adjusted)", adjusted_net_profit, ""),
           ("Actual Income (Adjusted)", adjusted_actual_income, ""),
           ("Net Profit (Raw)", raw_net_profit, "Raw "),
           ("Actual Income (Raw)", raw_actual_income, "Raw ")
       ]:
           if values is None or values.empty:
               results[f"{prefix}{metric}"] = "N/A"
               results[f"{prefix}{metric} Ascending"] = "N/A"
               continue
           try:
               latest_value = values.iloc[-1]
               historical_values = values.iloc[:-1]
               if historical_values.empty:
                   results[f"{prefix}{metric}"] = "N/A"
                   results[f"{prefix}{metric} Ascending"] = "N/A"
               else:
                   is_highest = latest_value >= historical_values.max()
                   results[f"{prefix}{metric}"] = "PASS" if is_highest else "FAIL"
                   is_asc = FinancialAnalyzer.is_ascending(values)
                   results[f"{prefix}{metric} Ascending"] = "PASS" if is_asc else "FAIL"
           except Exception:
               results[f"{prefix}{metric}"] = "N/A"
               results[f"{prefix}{metric} Ascending"] = "N/A"
       return results

# Stock screener module
class StockScreener:
   @staticmethod
   @st.cache_data
   def calculate_sma_and_screen(_db_manager, symbols_df, start_date, end_date, default_cooldown, _analyzer, _data_fetcher):
       bullish_stocks = []
       bearish_stocks = []
       v20_stocks = []
       week_52_stocks = []
       total_stocks = len(symbols_df)
       progress_bar = st.progress(0)
       processed_stocks = 0
       for index, row in symbols_df.iterrows():
           symbol = row['Symbol']
           stock_type = row.get('Type', '').lower()
           ticker = symbol.replace('.NS', '')
           try:
               if not symbol.endswith('.NS'):
                   symbol = symbol + '.NS'
               #stock_data = yf.download(symbol, start=start_date, end=end_date, progress=False)
               stock_data = yf.download(symbol, start=start_date, end=end_date, progress=False, auto_adjust=False)
               if stock_data.empty:
                   st.warning(f"No stock data found for {symbol}. Skipping...")
                   processed_stocks += 1
                   progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                   continue
               if len(stock_data) < 200:
                   st.warning(f"Insufficient data points ({len(stock_data)}) for {symbol} to calculate 200 SMA. Skipping...")
                   processed_stocks += 1
                   progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                   continue
               if isinstance(stock_data.columns, pd.MultiIndex):
                   stock_data.columns = stock_data.columns.get_level_values(0)
               if 'Close' not in stock_data.columns:
                   st.warning(f"No 'Close' column found for {symbol}. Columns: {list(stock_data.columns)}. Skipping...")
                   processed_stocks += 1
                   progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                   continue
               stock_data['SMA_20'] = stock_data['Close'].rolling(window=20).mean()
               stock_data['SMA_50'] = stock_data['Close'].rolling(window=50).mean()
               stock_data['SMA_200'] = stock_data['Close'].rolling(window=200).mean()
               latest_data = stock_data.iloc[-1]
               try:
                   close_price = float(latest_data['Close']) if not isinstance(latest_data['Close'], pd.Series) else float(latest_data['Close'].iloc[0])
                   sma_20 = float(latest_data['SMA_20']) if not isinstance(latest_data['SMA_20'], pd.Series) else float(latest_data['SMA_20'].iloc[0])
                   sma_50 = float(latest_data['SMA_50']) if not isinstance(latest_data['SMA_50'], pd.Series) else float(latest_data['SMA_50'].iloc[0])
                   sma_200 = float(latest_data['SMA_200']) if not isinstance(latest_data['SMA_200'], pd.Series) else float(latest_data['SMA_200'].iloc[0])
               except (KeyError, IndexError, ValueError) as e:
                   st.warning(f"Error extracting scalar values for {symbol}: {str(e)}. Columns: {list(stock_data.columns)}. Skipping...")
                   processed_stocks += 1
                   progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                   continue
               if pd.isna(close_price) or pd.isna(sma_20) or pd.isna(sma_50) or pd.isna(sma_200):
                   st.warning(f"Missing values for {symbol} (Close: {close_price}, SMA_20: {sma_20}, SMA_50: {sma_50}, SMA_200: {sma_200}). Skipping...")
                   processed_stocks += 1
                   progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                   continue
               quarterly_data, yearly_data, error = _analyzer.load_from_csv(ticker) if not st.session_state.force_refresh else (None, None, None)
               if error or quarterly_data is None or yearly_data is None:
                   quarterly_data, yearly_data, error = _data_fetcher.scrape_screener_data(ticker)
                   if not error:
                       _analyzer.save_to_csv(quarterly_data, yearly_data, ticker)
                       st.session_state.quarterly_data[ticker] = quarterly_data
                       st.session_state.yearly_data[ticker] = yearly_data
                   else:
                       st.warning(f"Financial data error for {ticker}: {error}")
               is_finance = st.session_state.finance_override or _analyzer.is_finance_company(quarterly_data)
               qoq_results = _analyzer.check_highest_historical(quarterly_data, True, is_finance)
               same_quarter_results = _analyzer.check_same_quarter_comparison(quarterly_data, st.session_state.enable_same_quarter)
               yoy_results = _analyzer.check_highest_historical(yearly_data, False, is_finance)
               stock_info = {
                   'Symbol': symbol,
                   'Type': stock_type,
                   'Close Price': round(close_price, 2),
                   '20 SMA': round(sma_20, 2),
                   '50 SMA': round(sma_50, 2),
                   '200 SMA': round(sma_200, 2),
                   'Company Type': 'Finance' if is_finance else 'Non-Finance',
                   'QOQ Net Profit (Adjusted)': qoq_results.get('Net Profit (Adjusted)', 'N/A'),
                   'QOQ Actual Income (Adjusted)': qoq_results.get('Actual Income (Adjusted)', 'N/A'),
                   'QOQ Net Profit (Raw)': qoq_results.get('Raw Net Profit (Raw)', 'N/A'),
                   'QOQ Actual Income (Raw)': qoq_results.get('Raw Actual Income (Raw)', 'N/A'),
                   'QOQ Net Profit Ascending (Adjusted)': qoq_results.get('Net Profit (Adjusted) Ascending', 'N/A'),
                   'QOQ Actual Income Ascending (Adjusted)': qoq_results.get('Actual Income (Adjusted) Ascending', 'N/A'),
                   'QOQ Net Profit Ascending (Raw)': qoq_results.get('Raw Net Profit (Raw) Ascending', 'N/A'),
                   'QOQ Actual Income Ascending (Raw)': qoq_results.get('Raw Actual Income (Raw) Ascending', 'N/A'),
                   'Same Quarter Net Profit (Adjusted)': same_quarter_results.get('Same Quarter Net Profit (Adjusted)', 'N/A'),
                   'Same Quarter Net Profit (Raw)': same_quarter_results.get('Same Quarter Net Profit (Raw)', 'N/A'),
                   'YOY Net Profit (Adjusted)': yoy_results.get('Net Profit (Adjusted)', 'N/A'),
                   'YOY Actual Income (Adjusted)': yoy_results.get('Actual Income (Adjusted)', 'N/A'),
                   'YOY Net Profit (Raw)': yoy_results.get('Raw Net Profit (Raw)', 'N/A'),
                   'YOY Actual Income (Raw)': yoy_results.get('Raw Actual Income (Raw)', 'N/A'),
                   'YOY Net Profit Ascending (Adjusted)': yoy_results.get('Net Profit (Adjusted) Ascending', 'N/A'),
                   'YOY Actual Income Ascending (Adjusted)': yoy_results.get('Actual Income (Adjusted) Ascending', 'N/A'),
                   'YOY Net Profit Ascending (Raw)': yoy_results.get('Raw Net Profit (Raw) Ascending', 'N/A'),
                   'YOY Actual Income Ascending (Raw)': yoy_results.get('Raw Actual Income (Raw) Ascending', 'N/A'),
                   'Error': error or 'None'
               }
               if sma_200 > sma_50 and sma_50 > sma_20:
                   stock_info['Strategy'] = 'SMA'
                   bullish_stocks.append(stock_info)
                   _db_manager.add_stock(symbol, 'SMA', close_price, sma_20, sma_50, default_cooldown)
               elif sma_200 < sma_50 and sma_50 < sma_20:
                   stock_info['Strategy'] = 'Bearish'
                   bearish_stocks.append(stock_info)
               if stock_type in ['v200', 'v40', 'v40next']:
                   momentum_period, momentum_gain, start_date_momentum, end_date_momentum, v20_low_price = StockScreener.check_v20_strategy(stock_data, stock_type, sma_200)
                   if momentum_gain >= 20:
                       near_v20_low = abs(close_price - v20_low_price) / v20_low_price <= 0.02 if v20_low_price else False
                       distance_from_v20_low = ((close_price - v20_low_price) / v20_low_price * 100) if v20_low_price else None
                       v20_info = stock_info.copy()
                       v20_info.update({
                           'Strategy': 'V20',
                           'Momentum Gain (%)': round(momentum_gain, 2),
                           'Momentum Duration': f"{start_date_momentum} to {end_date_momentum}",
                           'V20 Low Price': round(v20_low_price, 2) if v20_low_price else None,
                           'Near V20 Low (Within 2%)': 'Yes' if near_v20_low else 'No',
                           'Distance from V20 Low (%)': round(distance_from_v20_low, 2) if distance_from_v20_low is not None else 'N/A'
                       })
                       v20_stocks.append(v20_info)
                       v20_high_price = stock_data['High'].loc[start_date_momentum:end_date_momentum].max() if v20_low_price else None
                       if v20_low_price and v20_high_price:
                           _db_manager.add_stock(symbol, 'V20', close_price, v20_low_price, v20_high_price, default_cooldown)
               if stock_type == 'v40':
                   week_52_low, week_52_high = stock_data['Low'].min(), stock_data['High'].max()
                   near_52w_low = abs(close_price - week_52_low) / week_52_low <= 0.02 if week_52_low else False
                   distance_from_52w_low = ((close_price - week_52_low) / week_52_low * 100) if week_52_low else None
                   week_52_info = stock_info.copy()
                   week_52_info.update({
                       'Strategy': '52WLowHigh',
                       '52W Low Price': round(week_52_low, 2) if week_52_low else None,
                       '52W High Price': round(week_52_high, 2) if week_52_high else None,
                       'Near 52W Low (Within 2%)': 'Yes' if near_52w_low else 'No',
                       'Distance from 52W Low (%)': round(distance_from_52w_low, 2) if distance_from_52w_low is not None else 'N/A'
                   })
                   week_52_stocks.append(week_52_info)
                   if week_52_low and week_52_high:
                       _db_manager.add_stock(symbol, '52WLowHigh', close_price, week_52_low, week_52_high, default_cooldown)
               processed_stocks += 1
               progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
           except Exception as e:
               st.warning(f"Error processing {symbol}: {str(e)}")
               processed_stocks += 1
               progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
       return pd.DataFrame(bullish_stocks), pd.DataFrame(bearish_stocks), pd.DataFrame(v20_stocks), pd.DataFrame(week_52_stocks)

   @staticmethod
   def check_v20_strategy(stock_data, stock_type, sma_200):
       try:
           momentum_period = None
           momentum_gain = 0
           start_date_momentum = None
           end_date_momentum = None
           v20_low_price = None
           for i in range(len(stock_data) - 1, -1, -1):
               current_candle = stock_data.iloc[i]
               if current_candle['Close'] > current_candle['Open']:
                   for j in range(i - 1, -1, -1):
                       prev_candle = stock_data.iloc[j]
                       gain_percent = ((current_candle['High'] - prev_candle['Low']) / prev_candle['Low']) * 100
                       if gain_percent >= 20:
                           momentum_broken = False
                           for k in range(j + 1, i + 1):
                               if stock_data.iloc[k]['Close'] < stock_data.iloc[k]['Open']:
                                   momentum_broken = True
                                   break
                           if not momentum_broken:
                               if stock_type == 'v200' and prev_candle['Close'] >= stock_data.iloc[j]['SMA_200']:
                                   continue
                               momentum_period = i - j
                               momentum_gain = gain_percent
                               start_date_momentum = prev_candle.name.strftime('%Y-%m-%d')
                               end_date_momentum = current_candle.name.strftime('%Y-%m-%d')
                               v20_low_price = prev_candle['Low']
                               break
                   if momentum_period:
                       break
           return momentum_period, momentum_gain, start_date_momentum, end_date_momentum, v20_low_price
       except Exception as e:
           logging.error(f"Error in check_v20_strategy for {stock_type}: {e}")
           return None, 0, None, None, None

# Alert manager module
class AlertManager:
   def __init__(self, db_manager):
       self.db_manager = db_manager
       try:
           logging.info(f"Attempting to initialize Telegram bot with chat_id: {st.secrets['CHAT_ID']}")
           self.bot = telegram.Bot(
               token=st.secrets["TELEGRAM_TOKEN"],
               request=telegram.request.HTTPXRequest(
                   connection_pool_size=20,
                   read_timeout=10.0,
                   write_timeout=10.0,
                   connect_timeout=10.0
               )
           )
           self.chat_id = st.secrets["CHAT_ID"]
       except Exception as e:
           logging.error(f"Telegram configuration error: {e}. Please check secrets configuration.")
           st.error(f"Telegram configuration error: {e}. Please check secrets configuration.")
           self.bot = None
           self.chat_id = None

   async def send_telegram_message(self, message, symbol, strategy, notification_type, price):
       if self.bot is None:
           logging.error("Telegram bot not configured")
           st.error("Telegram bot not configured")
           with open("fallback_notifications.txt", "a") as f:
               f.write(f"{datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
           self.db_manager.add_notification(symbol, strategy, notification_type, price, int(time.time()))
           return
       try:
           await self.bot.send_message(chat_id=self.chat_id, text=message)
           self.db_manager.add_notification(symbol, strategy, notification_type, price, int(time.time()))
           logging.info(f"Sent Telegram message: {message}")
       except Exception as e:
           logging.error(f"Failed to send Telegram message: {e}")
           st.error(f"Failed to send Telegram message: {e}")
           with open("fallback_notifications.txt", "a") as f:
               f.write(f"{datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
           self.db_manager.add_notification(symbol, strategy, notification_type, price, int(time.time()))

   async def check_prices(self, db_manager, data_fetcher):
       df = db_manager.get_enabled_stocks()
       current_time = time.time()
       for _, row in df.iterrows():
           try:
               current_price, v20_range, week_52_range = data_fetcher.get_stock_data(row['symbol'])
               if current_price is None:
                   continue
               if current_time - row['created_at'] < 60 and row['alert_triggered'] == 0:
                   if abs(current_price - row['alert_price']) / row['alert_price'] <= 0.01:
                       logging.info(f"Skipping initial alert notification for {row['symbol']} as price is already at/beyond alert price")
                       continue
               if row['alert_price'] > 0 and row['alert_triggered'] == 0:
                   last_alert_time = row['last_notified_alert'] if row['last_notified_alert'] > 0 else row['created_at']
                   if current_time - last_alert_time >= row['notification_cooldown'] and current_price <= row['alert_price'] + 0.01:
                       strategy = '52WLowHigh' if row['strategy'] == '52WLowHigh' else row['strategy']
                       message = f"🚨 {strategy} Buy Alert: {row['symbol']} hit buy price ₹{row['alert_price']:.2f}! Current: ₹{current_price:.2f}"
                       await self.send_telegram_message(message, row['symbol'], strategy, "Buy", current_price)
                       with sqlite3.connect('stock_alerts_modular.db') as conn:
                           c = conn.cursor()
                           c.execute("UPDATE stocks SET alert_triggered = 1, last_notified_alert = ? WHERE id = ?", (current_time, row['id']))
                           conn.commit()
                       logging.info(f"Buy alert triggered for {row['symbol']} at ₹{current_price:.2f}")
                       await asyncio.sleep(0.5)
               if row['target_price'] > 0 and row['alert_triggered'] == 1:
                   last_target_time = row['last_notified_target'] if row['last_notified_target'] > 0 else row['created_at']
                   if current_time - last_target_time >= row['notification_cooldown'] and current_price >= row['target_price'] - 0.01:
                       strategy = '52WLowHigh' if row['strategy'] == '52WLowHigh' else row['strategy']
                       message = f"🎯 {strategy} Sell Alert: {row['symbol']} hit target price ₹{row['target_price']:.2f}! Current: ₹{current_price:.2f}"
                       await self.send_telegram_message(message, row['symbol'], strategy, "Sell", current_price)
                       with sqlite3.connect('stock_alerts_modular.db') as conn:
                           c = conn.cursor()
                           c.execute("UPDATE stocks SET last_notified_target = ? WHERE id = ?", (current_time, row['id']))
                           conn.commit()
                       logging.info(f"Sell alert triggered for {row['symbol']} at ₹{current_price:.2f}")
                       await asyncio.sleep(0.5)
           except Exception as e:
               logging.error(f"Error checking {row['symbol']}: {e}")
               st.error(f"Error checking {row['symbol']}: {e}")

# UI components module
class UIManager:
   @staticmethod
   def copy_to_clipboard(df, table_name):
       try:
           df_string = df.to_csv(sep='\t', index=False)
           pyperclip.copy(df_string)
           st.success(f"{table_name} copied to clipboard! Paste into Excel or Google Sheets.")
       except Exception as e:
           st.error(f"Error copying {table_name} to clipboard: {e}. Please try again or check clipboard permissions.")

   @staticmethod
   def display_dataframe(df, title, highlight_v20=False, highlight_52w=False):
       if not df.empty:
           st.subheader(title)
           if highlight_v20:
               display_df = df.style.apply(UIManager.highlight_near_v20_low, axis=1)
           elif highlight_52w:
               display_df = df.style.apply(UIManager.highlight_near_52w_low, axis=1)
           else:
               display_df = df
           st.dataframe(display_df, use_container_width=True)
           csv = df.to_csv(index=False)
           st.download_button(
               label=f"Download {title} as CSV",
               data=csv,
               file_name=f"{title.lower().replace(' ', '_')}.csv",
               mime="text/csv",
               key=f"download_{title.lower().replace(' ', '_')}"
           )
           if st.button(f"Copy {title} to Clipboard", key=f"copy_{title.lower().replace(' ', '_')}"):
               UIManager.copy_to_clipboard(df, title)
       else:
           st.warning(f"No stocks match the {title.lower()} criteria.")

   @staticmethod
   def highlight_near_v20_low(row):
       if row['Near V20 Low (Within 2%)'] == 'Yes':
           return ['background-color: #90EE90'] * len(row)
       return [''] * len(row)

   @staticmethod
   def highlight_near_52w_low(row):
       if row['Near 52W Low (Within 2%)'] == 'Yes':
           return ['background-color: #90EE90'] * len(row)
       return [''] * len(row)

   @staticmethod
   def export_stocks_to_csv(db_manager, data_fetcher, ist):
       df = db_manager.get_all_stocks()
       df['days_to_target'] = df.apply(
           lambda row: (datetime.fromtimestamp(row['last_notified_target'], tz=ist) -
                        datetime.fromtimestamp(row['last_notified_alert'], tz=ist)).days
           if (row['last_notified_alert'] > 0 and row['last_notified_target'] > 0) else 0,
           axis=1
       )
       df['created_at'] = df['created_at'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
       df['last_notified_alert'] = df['last_notified_alert'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
       df['last_notified_target'] = df['last_notified_target'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
       current_prices = {}
       v20_low_prices = {}
       week_52_low_prices = {}
       for _, row in df.iterrows():
           current_price, v20_range, week_52_range = data_fetcher.get_stock_data(row['symbol'])
           current_prices[row['id']] = current_price if current_price else 0
           v20_low_prices[row['id']] = row['alert_price'] if row['strategy'] == 'V20' else None
           week_52_low_prices[row['id']] = row['alert_price'] if row['strategy'] == '52WLowHigh' else None
       df['current_price'] = df['id'].map(current_prices)
       df['v20_low_price'] = df['id'].map(v20_low_prices)
       df['week_52_low_price'] = df['id'].map(week_52_low_prices)
       df['distance_from_low'] = df.apply(
           lambda row: round(((row['current_price'] - row['v20_low_price']) / row['v20_low_price'] * 100), 2)
           if row['strategy'] == 'V20' and row['v20_low_price'] is not None and row['current_price'] != 0
           else round(((row['current_price'] - row['week_52_low_price']) / row['week_52_low_price'] * 100), 2)
           if row['strategy'] == '52WLowHigh' and row['week_52_low_price'] is not None and row['current_price'] != 0
           else 'N/A',
           axis=1
       )
       output = io.StringIO()
       df.to_csv(output, index=False, columns=[
           'symbol', 'strategy', 'initial_price', 'alert_price', 'target_price',
           'alert_triggered', 'created_at', 'current_price', 'v20_low_price',
           'week_52_low_price', 'distance_from_low', 'days_to_target'
       ])
       return output.getvalue()

   @staticmethod
   def export_notifications_to_csv(db_manager, ist):
       df = db_manager.get_notifications()
       df['timestamp'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
       output = io.StringIO()
       df.to_csv(output, index=False, columns=['symbol', 'strategy', 'notification_type', 'price', 'timestamp'])
       return output.getvalue()

# Main application
def main():
   st.set_page_config(page_title="Stock SMA, V20, 52WLowHigh Screener & Alert System", layout="wide")
   st.title("📈 Stock SMA, V20, 52WLowHigh Screener & Alert System")
   st.markdown("""
   This app screens stocks based on:
   - **SMA Strategy**: Bullish (200 SMA > 50 SMA > 20 SMA) and Bearish (200 SMA < 50 SMA < 20 SMA) trends.
   - **V20 Strategy**: 20%+ gain with green candles (v200 types below 200 SMA).
   - **52WLowHigh Strategy**: Buy signal at 52-week low, sell signal at 52-week high (v40 stocks only).
   - **Financial Metrics**: QoQ and YoY Net Profit/Actual Income (adjusted/raw, highest historical, ascending).
   It also manages stock alerts with Telegram notifications for buy/sell signals, stored persistently in a notifications table.
   Stocks found in screening results are automatically added to the alert system under their respective strategies.
   Upload a CSV with 'Symbol' and 'Type' (v200, v40, v40next, ML) columns to screen stocks.
   """)
   initialize_session_state()
   ist = pytz.timezone('Asia/Kolkata')
   db_manager = DatabaseManager()
   st.session_state.strategies = db_manager.get_strategies()
   data_fetcher = DataFetcher()
   analyzer = FinancialAnalyzer()
   alert_manager = AlertManager(db_manager)
   ui_manager = UIManager()
   screener = StockScreener()

   # Sidebar configuration
   st.sidebar.header("Configuration")
   check_interval = st.sidebar.slider("Price Check Interval (minutes)", 1, 60, 5, key="check_interval")
   default_cooldown = st.sidebar.number_input("Notification Cooldown (seconds)", min_value=30, max_value=86400, value=120, key="cooldown")
   #st.sidebar.checkbox("Override: Treat as Finance Company", key="finance_override")
   st.sidebar.checkbox("Enable Same Quarter Year-over-Year Net Profit Comparison", key="enable_same_quarter")
   st.sidebar.checkbox("Force Refresh Financial Data", key="force_refresh")
   st.sidebar.subheader("Date Range for SMA Calculation (Minimum 1 Years)")
   end_date = date.today()
   start_date = end_date - timedelta(days=360)
   start_date = st.sidebar.date_input("Start Date", value=start_date, min_value=end_date - timedelta(days=360), key="start_date")
   end_date = st.sidebar.date_input("End Date", value=end_date, key="end_date")

   # Manual notification
   st.sidebar.subheader("Manual Actions")
   if st.sidebar.button("Send Manual Notification", key="manual_notification"):
       asyncio.run(alert_manager.check_prices(db_manager, data_fetcher))
       st.sidebar.success("Manual notifications triggered!")

   # Bulk actions
   st.sidebar.subheader("Bulk Actions")
   if st.sidebar.button("Reset Alert Triggered for All Stocks", key="reset_alerts"):
       db_manager.reset_alerts()
       st.sidebar.success("Alert triggered reset for all stocks!")
       st.rerun()
   if st.sidebar.button("Delete All Stocks", key="delete_all_stocks"):
       db_manager.delete_all_stocks()
       st.sidebar.success("All stocks deleted from database!")
       st.rerun()
   if st.sidebar.button("Clear Notification History", key="clear_notifications"):
       db_manager.clear_notifications()
       st.sidebar.success("Notification history cleared!")
       st.rerun()

   # Strategy management
   st.sidebar.subheader("Manage Strategies")
   with st.form(key="add_strategy_form"):
       new_strategy = st.text_input("Add New Strategy", key="new_strategy")
       if st.form_submit_button("Add Strategy"):
           if new_strategy:
               db_manager.add_strategy(new_strategy)
               st.sidebar.success(f"Strategy '{new_strategy}' added!")
               st.session_state.strategies = db_manager.get_strategies()

   # Export stocks and notifications
   st.sidebar.subheader("Export Data")
   if st.sidebar.button("Export All Stocks to CSV", key="export_stocks"):
       csv_data = ui_manager.export_stocks_to_csv(db_manager, data_fetcher, ist)
       st.sidebar.download_button(
           label="Download Stocks CSV",
           data=csv_data,
           file_name=f"stock_alerts_{datetime.now(ist).strftime('%Y%m%d_%H%M%S')}.csv",
           mime="text/csv",
           key="download_stocks_csv"
       )
       logging.info("Exported all stocks to CSV")
   if st.sidebar.button("Export Notifications to CSV", key="export_notifications"):
       csv_data = ui_manager.export_notifications_to_csv(db_manager, ist)
       st.sidebar.download_button(
           label="Download Notifications CSV",
           data=csv_data,
           file_name=f"notifications_{datetime.now(ist).strftime('%Y%m%d_%H%M%S')}.csv",
           mime="text/csv",
           key="download_notifications_csv"
       )
       logging.info("Exported all notifications to CSV")

   # File uploader
   uploaded_file = st.file_uploader("Upload a CSV file with 'Symbol' and 'Type' (v200, v40, v40next, ML)", type=["csv"], key="file_uploader")
   symbols_df = pd.DataFrame()
   if uploaded_file is not None:
       symbols_df = pd.read_csv(uploaded_file)
       if 'Symbol' not in symbols_df.columns or 'Type' not in symbols_df.columns:
           st.error("CSV must contain 'Symbol' and 'Type' columns.")
           symbols_df = pd.DataFrame()
       else:
           st.success("CSV file uploaded successfully!")
   else:
       st.warning("Please upload a CSV file with stock symbols and types.")

   # Screen stocks
   if st.button("Screen Stocks", key="screen_stocks"):
       if not symbols_df.empty:
           with st.spinner("Fetching stock and financial data, calculating SMAs, V20, and 52WLowHigh strategies..."):
               bullish_df, bearish_df, v20_df, week_52_df = screener.calculate_sma_and_screen(db_manager, symbols_df, start_date, end_date, default_cooldown, analyzer, data_fetcher)
           ui_manager.display_dataframe(bullish_df, "SMA Strategy Stocks (200 SMA > 50 SMA > 20 SMA)")
           ui_manager.display_dataframe(bearish_df, "Bearish Stocks (200 SMA < 50 SMA < 20 SMA)")
           ui_manager.display_dataframe(v20_df, "V20 Strategy Stocks (20%+ Gain with Green Candles)", highlight_v20=True)
           ui_manager.display_dataframe(week_52_df, "52 Week Low-High Strategy Stocks (v40 Stocks)", highlight_52w=True)
           pass_columns = [
               'QOQ Net Profit (Adjusted)', 'QOQ Actual Income (Adjusted)', 'QOQ Net Profit (Raw)', 'QOQ Actual Income (Raw)',
               'QOQ Net Profit Ascending (Adjusted)', 'QOQ Actual Income Ascending (Adjusted)', 'QOQ Net Profit Ascending (Raw)', 'QOQ Actual Income Ascending (Raw)',
               'YOY Net Profit (Adjusted)', 'YOY Actual Income (Adjusted)', 'YOY Net Profit (Raw)', 'YOY Actual Income (Raw)',
               'YOY Net Profit Ascending (Adjusted)', 'YOY Actual Income Ascending (Adjusted)', 'YOY Net Profit Ascending (Raw)', 'YOY Actual Income Ascending (Raw)'
           ]
           if st.session_state.enable_same_quarter:
               pass_columns.extend(['Same Quarter Net Profit (Adjusted)', 'Same Quarter Net Profit (Raw)'])
           for df, title in [
               (bullish_df, "SMA Stocks with All PASS Criteria"),
               (bearish_df, "Bearish Stocks with All PASS Criteria"),
               (v20_df, "V20 Stocks with All PASS Criteria"),
               (week_52_df, "52 Week Low-High Stocks with All PASS Criteria")
           ]:
               if not df.empty:
                   pass_df = df[df[pass_columns].eq('PASS').all(axis=1)]
                   if not pass_df.empty:
                       display_columns = ['Symbol', 'Type', 'Close Price', '20 SMA', '50 SMA', '200 SMA', 'Company Type'] + pass_columns
                       if title == "V20 Stocks with All PASS Criteria":
                           display_columns.extend(['Momentum Gain (%)', 'Momentum Duration', 'V20 Low Price', 'Near V20 Low (Within 2%)', 'Distance from V20 Low (%)'])
                       elif title == "52 Week Low-High Stocks with All PASS Criteria":
                           display_columns.extend(['52W Low Price', '52W High Price', 'Near 52W Low (Within 2%)', 'Distance from 52W Low (%)'])
                       ui_manager.display_dataframe(pass_df[display_columns], title, 
                                                   highlight_v20=(title == "V20 Stocks with All PASS Criteria"),
                                                   highlight_52w=(title == "52 Week Low-High Stocks with All PASS Criteria"))

   # Stock input form
   st.subheader("Add New Stock Alert")
   with st.form(key="add_stock_form"):
       col1, col2 = st.columns(2)
       with col1:
           symbol = st.text_input("Stock Symbol", placeholder="BAJAJHFL.NS", key="symbol_input")
       with col2:
           strategy = st.selectbox("Strategy", st.session_state.strategies,
                                  index=st.session_state.strategies.index("V20") if "V20" in st.session_state.strategies else 0,
                                  key="strategy_select")
       submit_button = st.form_submit_button("Add Stock")
       if submit_button and symbol:
           with sqlite3.connect('stock_alerts_modular.db') as conn:
               c = conn.cursor()
               c.execute("SELECT id FROM stocks WHERE symbol = ? AND strategy = ?", (symbol.upper(), strategy))
               existing_stock = c.fetchone()
               if existing_stock:
                   st.error(f"Stock {symbol.upper()} with strategy '{strategy}' already exists!")
                   logging.warning(f"Attempted to add duplicate stock {symbol.upper()} with strategy {strategy}")
               else:
                   current_price, v20_range, week_52_range = data_fetcher.get_stock_data(symbol)
                   if strategy == "V20":
                       if current_price is not None and v20_range:
                           alert_price, target_price = v20_range
                           db_manager.add_stock(symbol, strategy, current_price, alert_price, target_price, default_cooldown)
                           st.success(f"Added {symbol.upper()} with V20 alert price ₹{alert_price:.2f} and target price ₹{target_price:.2f}")
                       else:
                           st.error(f"Invalid symbol {symbol.upper()}: No V20 range data available.")
                           logging.error(f"Failed to add stock {symbol.upper()}: No V20 range data")
                   elif strategy == "52WLowHigh":
                       if current_price is not None and week_52_range:
                           alert_price, target_price = week_52_range
                           db_manager.add_stock(symbol, strategy, current_price, alert_price, target_price, default_cooldown)
                           st.success(f"Added {symbol.upper()} with 52WLowHigh alert price ₹{alert_price:.2f} and target price ₹{target_price:.2f}")
                       else:
                           st.error(f"Invalid symbol {symbol.upper()}: No 52-week range data available.")
                           logging.error(f"Failed to add stock {symbol.upper()}: No 52-week range data")
                   else:
                       try:
                           stock_data = yf.download(symbol.upper(), start=start_date, end=end_date, progress=False)
                           if stock_data.empty:
                               st.error(f"No data found for {symbol.upper()}")
                           else:
                               stock_data['SMA_20'] = stock_data['Close'].rolling(window=20).mean()
                               stock_data['SMA_50'] = stock_data['Close'].rolling(window=50).mean()
                               stock_data['SMA_200'] = stock_data['Close'].rolling(window=200).mean()
                               latest_data = stock_data.iloc[-1]
                               sma_20 = float(latest_data['SMA_20'])
                               sma_50 = float(latest_data['SMA_50'])
                               sma_200 = float(latest_data['SMA_200'])
                               if sma_200 > sma_50 and sma_50 > sma_20:
                                   db_manager.add_stock(symbol, strategy, current_price, sma_20, sma_50, default_cooldown)
                                   st.success(f"Added {symbol.upper()} with SMA strategy (20 SMA ₹{sma_20:.2f}, 50 SMA ₹{sma_50:.2f})")
                                   logging.info(f"Added stock {symbol.upper()} with initial price ₹{current_price:.2f}, SMA strategy")
                               else:
                                   st.error(f"{symbol.upper()} does not meet SMA strategy criteria (200 SMA > 50 SMA > 20 SMA)")
                       except Exception as e:
                           st.error(f"Error processing {symbol.upper()} for SMA strategy: {e}")
                           logging.error(f"Error processing {symbol.upper()} for SMA strategy: {e}")

   
   # Display notifications
   st.subheader("Notification History")
   notifications_df = db_manager.get_notifications()
   if not notifications_df.empty:
       notifications_df['timestamp'] = notifications_df['timestamp'].apply(
           lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else ''
       )
       display_notifications = notifications_df[['symbol', 'strategy', 'notification_type', 'price', 'timestamp']]
       ui_manager.display_dataframe(display_notifications, "Notification History")
   else:
       st.warning("No notifications recorded yet.")

   # Scheduler
   def run_scheduler():
       schedule.every(check_interval).minutes.do(lambda: asyncio.run(alert_manager.check_prices(db_manager, data_fetcher)))
       while True:
           schedule.run_pending()
           time.sleep(60)

   if st.session_state.scheduler_thread:
       st.session_state.scheduler_thread = False
       scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
       scheduler_thread.start()

if __name__ == "__main__":
   main()
