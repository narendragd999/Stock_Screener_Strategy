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

# Configure logging
logging.basicConfig(
    filename='stock_screener_alerts.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set page configuration
st.set_page_config(page_title="Stock SMA, V20 Screener & Alert System", layout="wide")
st.title("📈 Stock SMA, V20 Screener & Alert System")
st.markdown("""
This app screens stocks based on:
- **SMA Strategy**: Bullish (200 SMA > 50 SMA > 20 SMA) and Bearish (200 SMA < 50 SMA < 20 SMA) trends.
- **V20 Strategy**: 20%+ gain with green candles (v200 types below 200 SMA).
- **Financial Metrics**: QoQ and YoY Net Profit/Actual Income (adjusted/raw, highest historical, ascending).
It also manages stock alerts with Telegram notifications for V20 buy/sell signals.
Stocks found in screening results are automatically added to the alert system under their respective strategies.
Upload a CSV with 'Symbol' and 'Type' (v200, v40, v40next, ML) columns to screen stocks.
""")

# Initialize session state
if 'quarterly_data' not in st.session_state:
    st.session_state.quarterly_data = {}
if 'yearly_data' not in st.session_state:
    st.session_state.yearly_data = {}
if 'finance_override' not in st.session_state:
    st.session_state.finance_override = False
if 'enable_same_quarter' not in st.session_state:
    st.session_state.enable_same_quarter = False
if 'force_refresh' not in st.session_state:
    st.session_state.force_refresh = False
if 'scheduler_thread' not in st.session_state:
    st.session_state.scheduler_thread = True

# Set timezone to IST
ist = pytz.timezone('Asia/Kolkata')

# Database setup
def init_db():
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    
    # Create stocks table
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
    
    # Check for missing columns and add them
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
    
    # Create strategies table
    c.execute('''CREATE TABLE IF NOT EXISTS strategies
                 (id TEXT PRIMARY KEY, name TEXT)''')
    
    # Create unique index to prevent duplicate stock-symbol-strategy combinations
    c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_strategy ON stocks (symbol, strategy)''')
    
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Telegram bot setup
try:
    TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
    CHAT_ID = st.secrets["CHAT_ID"]
    bot = telegram.Bot(token=TELEGRAM_TOKEN)
except Exception as e:
    st.error(f"Telegram configuration error: {e}. Please check secrets configuration.")
    TELEGRAM_TOKEN = None
    CHAT_ID = None
    bot = None

# Price checking and notification logic
async def send_telegram_message(message):
    if bot is None:
        logging.error("Telegram bot not configured")
        st.error("Telegram bot not configured")
        return
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
        logging.info(f"Sent Telegram message: {message}")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")
        st.error(f"Failed to send Telegram message: {e}")

# Function to fetch stock data using yfinance
def get_stock_data(symbol):
    try:
        ticker = symbol.upper() if symbol.endswith('.NS') else f"{symbol.upper()}.NS"
        stock = yf.Ticker(ticker)
        
        # Fetch current price
        info = stock.info
        current_price = info.get('regularMarketPrice', None)
        if current_price is None:
            logging.warning(f"No valid current price for {symbol}. Response: {info}")
            return None, None
        
        # Fetch 1.5 years of daily candlestick data
        hist = stock.history(period="1y2mo", interval="1d")
        if hist.empty:
            logging.warning(f"No historical data for {symbol}")
            return current_price, None
        
        # Find the most recent V20 range
        v20_range = None
        for i in range(len(hist) - 1, -1, -1):
            current_candle = hist.iloc[i]
            if current_candle['Close'] > current_candle['Open']:  # Green candle
                for j in range(i - 1, -1, -1):
                    prev_candle = hist.iloc[j]
                    gain_percent = ((current_candle['High'] - prev_candle['Low']) / prev_candle['Low']) * 100
                    if gain_percent >= 20:
                        momentum_broken = False
                        for k in range(j + 1, i + 1):
                            if hist.iloc[k]['Close'] < hist.iloc[k]['Open']:  # Red candle
                                momentum_broken = True
                                break
                        if not momentum_broken:
                            v20_range = (prev_candle['Low'], current_candle['High'])
                            break
                if v20_range:
                    break
        
        if not v20_range:
            logging.warning(f"No V20 range found for {symbol}")
            return current_price, None
        
        logging.info(f"Fetched data for {symbol}: Current price ₹{current_price:.2f}, V20 range low ₹{v20_range[0]:.2f}, high ₹{v20_range[1]:.2f}")
        return current_price, v20_range
    
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error for {symbol}: {e}")
        st.warning(f"Failed to fetch data for {symbol}: Invalid or empty response from Yahoo Finance.")
        return None, None
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        st.warning(f"Error fetching data for {symbol}: {e}")
        return None, None

# Function to scrape data from Screener.in
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
                            quarterly_data = parse_table(table)
                            if (quarterly_data is not None and 
                                not quarterly_data.empty and
                                quarterly_data.shape[1] >= 2 and 
                                quarterly_data.shape[0] >= 1 and 
                                str(quarterly_data.iloc[:, 1].values[0]).strip() != ""):
                                data_found = True
                        elif section.get('id') == 'profit-loss':
                            yearly_data = parse_table(table)
                            if (yearly_data is not None and 
                                not yearly_data.empty and
                                yearly_data.shape[0] >= 1):
                                data_found = True
            
            if data_found:
                break
           
        except requests.RequestException as e:
            error = f"Error fetching {url} for ticker {ticker}: {e}"
        
        if url == urls[1] and quarterly_data is None and yearly_data is None:
            error = f"No financial data tables with valid data found for ticker {ticker}"
    
    return quarterly_data, yearly_data, error

# Function to parse table data into a DataFrame
def parse_table(table):
    headers = [th.text.strip() for th in table.find('thead').find_all('th')]
    if not headers or len(headers) < 2:
        return None
    headers[0] = ''
    rows = []
    for tr in table.find('tbody').find_all('tr'):
        cells = [td.text.strip() for td in tr.find_all('td')]
        if cells and len(cells) == headers:
            rows.append(cells)
    df = pd.DataFrame(rows, columns=headers) if rows else None
    return df

# Function to load data from CSV files
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
        else:
            error = f"Yearly CSV file not found for {ticker}" if not error else error
    except Exception as e:
        error = f"Error loading yearly CSV for {ticker}: {e}" if not error else error
    
    return quarterly_data, yearly_data, error

# Function to save tables to CSV
def save_to_csv(quarterly_data, yearly_data, ticker):
    output_dir = 'output_tables'
    os.makedirs(output_dir, exist_ok=True)

    if quarterly_data is not None and not quarterly_data.empty:
        quarterly_data.to_csv(os.path.join(output_dir, f'{ticker}_quarterly_results.csv'), index=False)

    if yearly_data is not None and not yearly_data.empty:
        yearly_data.to_csv(os.path.join(output_dir, f'{ticker}_profit_loss.csv'), index=False)

# Function to determine if company is finance or non-finance
def is_finance_company(quarterly_data):
    if quarterly_data is None or quarterly_data.empty:
        return False
    return "Financing Profit" in quarterly_data.iloc[:, 0].values

# Function to find row by partial, case-insensitive, or fuzzy match
def find_row(data, row_name, threshold=0.8):
    possible_names = [row_name, row_name.replace(" ", ""), "Consolidated " + row_name, row_name + " (Consolidated)"]
    for name in possible_names:
        for index in data.index:
            if name.lower() in index.lower():
                return index
    matches = difflib.get_close_matches(row_name.lower(), [idx.lower() for idx in data.index], n=1, cutoff=threshold)
    return matches[0] if matches else None

# Function to clean numeric data
def clean_numeric(series):
    if isinstance(series, pd.DataFrame):
        series = series.iloc[0]
    elif not isinstance(series, pd.Series):
        series = pd.Series(series)
    
    series = series.astype(str).str.replace(',', '', regex=False).str.replace('[^0-9.-]', '', regex=True)
    return pd.to_numeric(series, errors='coerce').fillna(0)

# Function to adjust Net Profit and Actual Income
def adjust_non_finance(data, is_finance):
    if is_finance:
        net_profit_row = find_row(data, "Net Profit") or find_row(data, "Profit after tax")
        actual_income_row = find_row(data, "Actual Income") or net_profit_row
        net_profit = clean_numeric(data.loc[net_profit_row].iloc[1:]) if net_profit_row and net_profit_row in data.index else None
        actual_income = clean_numeric(data.loc[actual_income_row].iloc[1:]) if actual_income_row and actual_income_row in data.index else None
        return net_profit, actual_income

    net_profit_row = find_row(data, "Net Profit") or find_row(data, "Profit after tax")
    actual_income_row = find_row(data, "Actual Income") or net_profit_row
    other_income_row = find_row(data, "Other Income")

    net_profit = clean_numeric(data.loc[net_profit_row].iloc[1:]) if net_profit_row and net_profit_row in data.index else None
    actual_income = clean_numeric(data.loc[actual_income_row].iloc[1:]) if actual_income_row and actual_income_row in data.index else net_profit
    other_income = clean_numeric(data.loc[other_income_row].iloc[1:]) if other_income_row and other_income_row in data.index else pd.Series(0, index=net_profit.index if net_profit is not None else [])

    adjusted_net_profit = net_profit - other_income if net_profit is not None else None
    adjusted_actual_income = actual_income - other_income if actual_income is not None else adjusted_net_profit

    return adjusted_net_profit, adjusted_actual_income

# Function to check if data is in ascending order
def is_ascending(series):
    if series is None or series.empty:
        return False
    return all(series[i] <= series[i + 1] for i in range(len(series) - 1))

# Function to extract quarter and year from column name
def extract_quarter_year(column):
    patterns = [
        r'(\w+)\s+(\d{4})',  # e.g., "Mar 2025"
        r'(\w+)-(\d{2})',    # e.g., "Mar-25"
        r'(\w+)\s*\'(\d{2})' # e.g., "Mar'25"
    ]
    column = column.strip()
    for pattern in patterns:
        match = re.match(pattern, column)
        if match:
            quarter, year = match.groups()
            year = int(year) if len(year) == 4 else int("20" + year)
            return quarter, year
    return None, None

# Function to check same-quarter comparison
def check_same_quarter_comparison(data, enable_same_quarter):
    if not enable_same_quarter or data is None or data.empty:
        return {
            'Same Quarter Net Profit (Adjusted)': 'N/A',
            'Same Quarter Net Profit (Raw)': 'N/A'
        }
    
    if '' not in data.columns:
        return {
            'Same Quarter Net Profit (Adjusted)': 'N/A',
            'Same Quarter Net Profit (Raw)': 'N/A'
        }
    
    data = data.set_index('')
    adjusted_net_profit, _ = adjust_non_finance(data, is_finance_company(data))
    raw_net_profit = clean_numeric(data.loc[find_row(data, "Net Profit") or find_row(data, "Profit after tax")].iloc[1:]) if find_row(data, "Net Profit") or find_row(data, "Profit after tax") else None

    results = {
        'Same Quarter Net Profit (Adjusted)': 'N/A',
        'Same Quarter Net Profit (Raw)': 'N/A'
    }

    if adjusted_net_profit is None or raw_net_profit is None:
        return results

    try:
        latest_column = data.columns[-1]
        latest_quarter, latest_year = extract_quarter_year(latest_column)
        if latest_quarter is None or latest_year is None:
            return results

        prev_year_column = None
        for col in data.columns[:-1]:
            quarter, year = extract_quarter_year(col)
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

# Function to check if latest quarter/year is highest and in ascending order
def check_highest_historical(data, is_quarterly, is_finance):
    if data is None or data.empty:
        return {}
    
    if '' not in data.columns:
        return {
            'Net Profit (Adjusted)': 'N/A',
            'Actual Income (Adjusted)': 'N/A',
            'Raw Net Profit (Raw)': 'N/A',
            'Raw Actual Income (Raw)': 'N/A',
            'Net Profit (Adjusted) Ascending': 'N/A',
            'Actual Income (Adjusted) Ascending': 'N/A',
            'Raw Net Profit (Raw) Ascending': 'N/A',
            'Raw Actual Income (Raw) Ascending': 'N/A'
        }
    
    data = data.set_index('')
    adjusted_net_profit, adjusted_actual_income = adjust_non_finance(data, is_finance)
    raw_net_profit = clean_numeric(data.loc[find_row(data, "Net Profit") or find_row(data, "Profit after tax")].iloc[1:]) if find_row(data, "Net Profit") or find_row(data, "Profit after tax") else None
    raw_actual_income = clean_numeric(data.loc[find_row(data, "Actual Income") or find_row(data, "Net Profit") or find_row(data, "Profit after tax")].iloc[1:]) if find_row(data, "Actual Income") or find_row(data, "Net Profit") or find_row(data, "Profit after tax") else None

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
                is_asc = is_ascending(values)
                results[f"{prefix}{metric} Ascending"] = "PASS" if is_asc else "FAIL"
        except Exception:
            results[f"{prefix}{metric}"] = "N/A"
            results[f"{prefix}{metric} Ascending"] = "N/A"
    return results

# Function to add stock to alerts database
def add_stock_to_alerts(symbol, strategy, initial_price, alert_price, target_price, default_cooldown):
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    
    # Check for existing stock with same symbol and strategy
    c.execute("SELECT id FROM stocks WHERE symbol = ? AND strategy = ?", (symbol.upper(), strategy))
    existing_stock = c.fetchone()
    
    if existing_stock:
        logging.info(f"Stock {symbol.upper()} with strategy {strategy} already exists in alerts. Skipping auto-add.")
        conn.close()
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
        st.success(f"Auto-added {symbol.upper()} to alerts with strategy {strategy}")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Error auto-adding stock {symbol.upper()} to alerts: {e}")
        st.error(f"Error auto-adding {symbol.upper()} to alerts: {e}")
        conn.close()
        return False

# Function to calculate SMAs, V20 strategy, and financial metrics
@st.cache_data
def calculate_sma_and_screen(symbols_df, start_date, end_date, default_cooldown):
    bullish_stocks = []
    bearish_stocks = []
    v20_stocks = []
    total_stocks = len(symbols_df)
    
    # Initialize progress bar
    progress_bar = st.progress(0)
    processed_stocks = 0
    
    for index, row in symbols_df.iterrows():
        symbol = row['Symbol']
        stock_type = row.get('Type', '').lower()
        ticker = symbol.replace('.NS', '')  # For Screener.in scraping
        
        try:
            # Append .NS if no exchange suffix is present
            if not symbol.endswith('.NS'):
                symbol = symbol + '.NS'
            
            # Download historical stock data
            stock_data = yf.download(symbol, start=start_date, end=end_date, progress=False)
            if stock_data.empty:
                st.warning(f"No stock data found for {symbol}. Skipping...")
                processed_stocks += 1
                progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                continue
                
            # Ensure enough data for 200 SMA
            if len(stock_data) < 200:
                st.warning(f"Insufficient data points ({len(stock_data)}) for {symbol} to calculate 200 SMA. Skipping...")
                processed_stocks += 1
                progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                continue
                
            # Handle multi-level columns if present
            if isinstance(stock_data.columns, pd.MultiIndex):
                stock_data.columns = stock_data.columns.get_level_values(0)
            
            # Verify 'Close' column exists
            if 'Close' not in stock_data.columns:
                st.warning(f"No 'Close' column found for {symbol}. Columns: {list(stock_data.columns)}. Skipping...")
                processed_stocks += 1
                progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                continue
                
            # Calculate SMAs
            stock_data['SMA_20'] = stock_data['Close'].rolling(window=20).mean()
            stock_data['SMA_50'] = stock_data['Close'].rolling(window=50).mean()
            stock_data['SMA_200'] = stock_data['Close'].rolling(window=200).mean()
            
            # Get the latest values
            latest_data = stock_data.iloc[-1]
            
            # Extract scalar values
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
                
            # Check if SMAs are valid (not NaN)
            if pd.isna(close_price) or pd.isna(sma_20) or pd.isna(sma_50) or pd.isna(sma_200):
                st.warning(f"Missing values for {symbol} (Close: {close_price}, SMA_20: {sma_20}, SMA_50: {sma_50}, SMA_200: {sma_200}). Skipping...")
                processed_stocks += 1
                progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                continue
                
            # Financial data processing
            quarterly_data, yearly_data, error = None, None, None
            if not st.session_state.force_refresh:
                quarterly_data, yearly_data, error = load_from_csv(ticker)
            else:
                quarterly_data, yearly_data, error = None, None, None

            if error or quarterly_data is None or yearly_data is None:
                quarterly_data, yearly_data, error = scrape_screener_data(ticker)
                if not error:
                    save_to_csv(quarterly_data, yearly_data, ticker)
                    st.session_state.quarterly_data[ticker] = quarterly_data
                    st.session_state.yearly_data[ticker] = yearly_data
                else:
                    st.warning(f"Financial data error for {ticker}: {error}")

            is_finance = st.session_state.finance_override or is_finance_company(quarterly_data)
            qoq_results = check_highest_historical(quarterly_data, True, is_finance)
            same_quarter_results = check_same_quarter_comparison(quarterly_data, st.session_state.enable_same_quarter)
            yoy_results = check_highest_historical(yearly_data, False, is_finance)

            # Base stock data
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
            
            # Screen for SMA strategy (Bullish: 200 SMA > 50 SMA > 20 SMA)
            if sma_200 > sma_50 and sma_50 > sma_20:
                stock_info['Strategy'] = 'SMA'
                bullish_stocks.append(stock_info)
                # Auto-add to alerts
                add_stock_to_alerts(symbol, 'SMA', close_price, sma_20, sma_50, default_cooldown)
                
            # Screen for Bearish trend: 200 SMA < 50 SMA < 20 SMA
            elif sma_200 < sma_50 and sma_50 < sma_20:
                stock_info['Strategy'] = 'Bearish'
                bearish_stocks.append(stock_info)
                
            # V20 Strategy: Check for 20%+ gain with green candles
            if stock_type in ['v200', 'v40', 'v40next']:
                momentum_period, momentum_gain, start_date_momentum, end_date_momentum, v20_low_price = check_v20_strategy(stock_data, stock_type, sma_200)
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
                    # Auto-add to alerts
                    v20_high_price = stock_data['High'].loc[start_date_momentum:end_date_momentum].max() if v20_low_price else None
                    if v20_low_price and v20_high_price:
                        add_stock_to_alerts(symbol, 'V20', close_price, v20_low_price, v20_high_price, default_cooldown)
                
            processed_stocks += 1
            progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
                
        except Exception as e:
            st.warning(f"Error processing {symbol}: {str(e)}")
            processed_stocks += 1
            progress_bar.progress(min(processed_stocks / total_stocks, 1.0))
    
    return pd.DataFrame(bullish_stocks), pd.DataFrame(bearish_stocks), pd.DataFrame(v20_stocks)

# Function to check V20 strategy
def check_v20_strategy(stock_data, stock_type, latest_sma_200):
    if len(stock_data) < 2:
        return None, 0, None, None, None
    
    stock_data['Gain'] = ((stock_data['Close'] - stock_data['Open']) / stock_data['Open']) * 100
    stock_data['Is_Green'] = stock_data['Close'] > stock_data['Open']
    
    for i in range(len(stock_data)):
        if stock_data['Gain'].iloc[i] >= 20 and stock_data['Is_Green'].iloc[i]:
            candle_date = stock_data.index[i].strftime('%Y-%m-%d')
            if stock_type == 'v200':
                if stock_data['High'].iloc[i] < stock_data['SMA_200'].iloc[i]:
                    return "Single Candle", stock_data['Gain'].iloc[i], candle_date, candle_date, stock_data['Open'].iloc[i]
            else:
                return "Single Candle", stock_data['Gain'].iloc[i], candle_date, candle_date, stock_data['Open'].iloc[i]
    
    max_gain = 0
    start_idx = 0
    current_start = 0
    cumulative_gain = 0
    is_green_sequence = True
    
    for i in range(1, len(stock_data)):
        daily_gain = ((stock_data['Close'].iloc[i] - stock_data['Open'].iloc[i]) / stock_data['Open'].iloc[i]) * 100
        if stock_data['Is_Green'].iloc[i]:
            if is_green_sequence:
                cumulative_gain = ((stock_data['Close'].iloc[i] - stock_data['Open'].iloc[current_start]) / stock_data['Open'].iloc[current_start]) * 100
                if cumulative_gain >= 20:
                    if stock_type == 'v200':
                        if stock_data['High'].iloc[current_start:i+1].max() < stock_data['SMA_200'].iloc[current_start:i+1].min():
                            max_gain = cumulative_gain
                            start_idx = current_start
                    else:
                        max_gain = cumulative_gain
                        start_idx = current_start
        else:
            is_green_sequence = False
            cumulative_gain = 0
            current_start = i
        if not is_green_sequence:
            is_green_sequence = stock_data['Is_Green'].iloc[i]
            if is_green_sequence:
                current_start = i
                cumulative_gain = 0
    
    if max_gain >= 20:
        start_date = stock_data.index[start_idx].strftime('%Y-%m-%d')
        end_date = stock_data.index[-1].strftime('%Y-%m-%d')
        return "Continuous Green Candles", max_gain, start_date, end_date, stock_data['Low'].iloc[start_idx]
    
    return None, 0, None, None, None

# Function to copy DataFrame to clipboard
def copy_to_clipboard(df, table_name):
    try:
        df_string = df.to_csv(sep='\t', index=False)
        pyperclip.copy(df_string)
        st.success(f"{table_name} copied to clipboard! Paste into Excel or Google Sheets.")
    except Exception as e:
        st.error(f"Error copying {table_name} to clipboard: {e}. Please try again or check clipboard permissions.")

# Sidebar for configuration
st.sidebar.header("Configuration")
check_interval = st.sidebar.slider("Price Check Interval (minutes)", 1, 60, 5, key="check_interval")
default_cooldown = st.sidebar.number_input("Notification Cooldown (seconds)", min_value=30, max_value=86400, value=600, key="cooldown")
st.sidebar.checkbox("Override: Treat as Finance Company", key="finance_override")
st.sidebar.checkbox("Enable Same Quarter Year-over-Year Net Profit Comparison", key="enable_same_quarter")
st.sidebar.checkbox("Force Refresh Financial Data", key="force_refresh")

# Date range selection
st.sidebar.subheader("Date Range for SMA Calculation (Minimum 1.5 Years)")
end_date = date.today()
start_date = end_date - timedelta(days=548)
start_date = st.sidebar.date_input("Start Date", value=start_date, min_value=end_date - timedelta(days=548), key="start_date")
end_date = st.sidebar.date_input("End Date", value=end_date, key="end_date")

# Manual notification button
st.sidebar.subheader("Manual Actions")
if st.sidebar.button("Send Manual Notification", key="manual_notification"):
    conn = sqlite3.connect('stock_alerts.db')
    df = pd.read_sql_query("SELECT * FROM stocks WHERE enabled = 1", conn)
    conn.close()
    current_time = time.time()
    for _, row in df.iterrows():
        current_price, _ = get_stock_data(row['symbol'])
        if current_price:
            # Check alert price (buy signal)
            if row['alert_price'] > 0 and row['alert_triggered'] == 0:
                last_alert_time = row['last_notified_alert'] if row['last_notified_alert'] > 0 else row['created_at']
                if current_time - last_alert_time >= row['notification_cooldown'] and current_price <= row['alert_price'] + 0.01:
                    message = f"🚨 {row['strategy']} Buy Alert: {row['symbol']} hit buy price ₹{row['alert_price']:.2f}! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    logging.info(f"Manual buy alert triggered for {row['symbol']} at ₹{current_price:.2f}")

            # Check target price (sell signal) only after alert is triggered
            if row['target_price'] > 0 and row['alert_triggered'] == 1:
                last_target_time = row['last_notified_target'] if row['last_notified_target'] > 0 else row['created_at']
                if current_time - last_target_time >= row['notification_cooldown'] and current_price >= row['target_price'] - 0.01:
                    message = f"🎯 {row['strategy']} Sell Alert: {row['symbol']} hit target price ₹{row['target_price']:.2f}! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    logging.info(f"Manual sell alert triggered for {row['symbol']} at ₹{current_price:.2f}")

# Bulk actions in sidebar
st.sidebar.subheader("Bulk Actions")
if st.sidebar.button("Reset Alert Triggered for All Stocks", key="reset_alerts"):
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    c.execute("UPDATE stocks SET alert_triggered = 0, last_notified_alert = 0")
    conn.commit()
    conn.close()
    st.sidebar.success("Alert triggered reset for all stocks!")
    logging.info("Reset alert triggered for all stocks")
    st.rerun()

# Add under the "Bulk Actions" section in the sidebar
st.sidebar.subheader("Bulk Actions")
if st.sidebar.button("Delete All Stocks", key="delete_all_stocks"):
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    c.execute("DELETE FROM stocks")
    conn.commit()
    conn.close()
    st.sidebar.success("All stocks deleted from database!")
    logging.info("Deleted all stocks from database")
    st.rerun()
    
# Strategy management
st.sidebar.subheader("Manage Strategies")
new_strategy = st.sidebar.text_input("Add New Strategy", key="new_strategy")
if st.sidebar.button("Add Strategy", key="add_strategy"):
    if new_strategy:
        conn = sqlite3.connect('stock_alerts.db')
        c = conn.cursor()
        c.execute("INSERT INTO strategies (id, name) VALUES (?, ?)", (str(uuid.uuid4()), new_strategy))
        conn.commit()
        conn.close()
        st.sidebar.success(f"Strategy '{new_strategy}' added!")
        logging.info(f"Added strategy: {new_strategy}")

# Load strategies
conn = sqlite3.connect('stock_alerts.db')
c = conn.cursor()
c.execute("SELECT name FROM strategies")
strategies = [row[0] for row in c.fetchall()]
conn.close()
if not strategies:
    strategies = ["SMA", "V20"]

# Export stocks to CSV
def export_stocks_to_csv():
    conn = sqlite3.connect('stock_alerts.db')
    df = pd.read_sql_query("SELECT * FROM stocks", conn)
    conn.close()
    
    # Calculate days_to_target
    df['days_to_target'] = df.apply(
        lambda row: (datetime.fromtimestamp(row['last_notified_target'], tz=ist) - 
                     datetime.fromtimestamp(row['last_notified_alert'], tz=ist)).days 
        if (row['last_notified_alert'] > 0 and row['last_notified_target'] > 0) else 0, 
        axis=1
    )
    
    # Convert timestamps to human-readable format
    df['created_at'] = df['created_at'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
    df['last_notified_alert'] = df['last_notified_alert'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
    df['last_notified_target'] = df['last_notified_target'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
    
    # Calculate distance from V20 low for V20 strategy
    current_prices = {}
    v20_low_prices = {}
    for _, row in df.iterrows():
        current_price, v20_range = get_stock_data(row['symbol'])
        current_prices[row['id']] = current_price if current_price else 0
        v20_low_prices[row['id']] = row['alert_price'] if row['strategy'] == 'V20' else None
    
    df['current_price'] = df['id'].map(current_prices)
    df['v20_low_price'] = df['id'].map(v20_low_prices)
    df['distance_from_v20_low'] = df.apply(
        lambda row: round(((row['current_price'] - row['v20_low_price']) / row['v20_low_price'] * 100), 2)
        if row['strategy'] == 'V20' and row['v20_low_price'] is not None and row['current_price'] != 0 else 'N/A',
        axis=1
    )
    
    output = io.StringIO()
    df.to_csv(output, index=False, columns=[
        'symbol', 'strategy', 'initial_price', 'alert_price', 'target_price', 
        'alert_triggered', 'created_at', 'current_price', 'v20_low_price', 
        'distance_from_v20_low', 'days_to_target'
    ])
    return output.getvalue()

st.sidebar.subheader("Export Stocks")
if st.sidebar.button("Export All Stocks to CSV", key="export_stocks"):
    csv_data = export_stocks_to_csv()
    st.sidebar.download_button(
        label="Download Stocks CSV",
        data=csv_data,
        file_name=f"stock_alerts_{datetime.now(ist).strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        key="download_stocks_csv"
    )
    logging.info("Exported all stocks to CSV")

# File uploader
uploaded_file = st.file_uploader("Upload a CSV file with 'Symbol' and 'Type' (v200, v40, v40next, ML)", type=["csv"], key="file_uploader")
if uploaded_file is not None:
    symbols_df = pd.read_csv(uploaded_file)
    if 'Symbol' not in symbols_df.columns or 'Type' not in symbols_df.columns:
        st.error("CSV must contain 'Symbol' and 'Type' columns.")
        symbols_df = pd.DataFrame()
else:
    st.warning("Please upload a CSV file with stock symbols and types.")
    symbols_df = pd.DataFrame()

# Screen stocks when button is clicked
if st.button("Screen Stocks", key="screen_stocks"):
    if not symbols_df.empty:
        with st.spinner("Fetching stock and financial data, calculating SMAs and V20 strategy..."):
            bullish_df, bearish_df, v20_df = calculate_sma_and_screen(symbols_df, start_date, end_date, default_cooldown)
        
        # Display Bullish Stocks (SMA Strategy)
        st.subheader("SMA Strategy Stocks (200 SMA > 50 SMA > 20 SMA)")
        if not bullish_df.empty:
            st.dataframe(bullish_df, use_container_width=True)
            csv = bullish_df.to_csv(index=False)
            st.download_button(
                label="Download SMA Strategy Stocks as CSV",
                data=csv,
                file_name="sma_stocks.csv",
                mime="text/csv",
                key="download_sma_stocks"
            )
            if st.button("Copy SMA Strategy Stocks to Clipboard", key="copy_sma_stocks"):
                copy_to_clipboard(bullish_df, "SMA Strategy Stocks")
        else:
            st.warning("No stocks match the SMA strategy criteria.")
        
        # Display Bearish Stocks
        st.subheader("Bearish Stocks (200 SMA < 50 SMA < 20 SMA)")
        if not bearish_df.empty:
            st.dataframe(bearish_df, use_container_width=True)
            csv = bearish_df.to_csv(index=False)
            st.download_button(
                label="Download Bearish Stocks as CSV",
                data=csv,
                file_name="bearish_stocks.csv",
                mime="text/csv",
                key="download_bearish_stocks"
            )
            if st.button("Copy Bearish Stocks to Clipboard", key="copy_bearish_stocks"):
                copy_to_clipboard(bearish_df, "Bearish Stocks")
        else:
            st.warning("No stocks match the bearish criteria.")
        
        # Display V20 Strategy Stocks
        st.subheader("V20 Strategy Stocks (20%+ Gain with Green Candles)")
        if not v20_df.empty:
            def highlight_near_v20_low(row):
                if row['Near V20 Low (Within 2%)'] == 'Yes':
                    return ['background-color: #90EE90'] * len(row)
                return [''] * len(row)
            
            st.dataframe(v20_df.style.apply(highlight_near_v20_low, axis=1), use_container_width=True)
            csv = v20_df.to_csv(index=False)
            st.download_button(
                label="Download V20 Strategy Stocks as CSV",
                data=csv,
                file_name="v20_stocks.csv",
                mime="text/csv",
                key="download_v20_stocks"
            )
            if st.button("Copy V20 Strategy Stocks to Clipboard", key="copy_v20_stocks"):
                copy_to_clipboard(v20_df, "V20 Strategy Stocks")
        else:
            st.warning("No stocks match the V20 strategy criteria.")
        
        # Display Tickers with All PASS Criteria
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
            (v20_df, "V20 Stocks with All PASS Criteria")
        ]:
            if not df.empty:
                pass_df = df[df[pass_columns].eq('PASS').all(axis=1)]
                if not pass_df.empty:
                    st.subheader(title)
                    display_columns = ['Symbol', 'Type', 'Close Price', '20 SMA', '50 SMA', '200 SMA', 'Company Type'] + pass_columns
                    if title == "V20 Stocks with All PASS Criteria":
                        display_columns.extend(['Momentum Gain (%)', 'Momentum Duration', 'V20 Low Price', 'Near V20 Low (Within 2%)', 'Distance from V20 Low (%)'])
                    st.dataframe(
                        pass_df[display_columns].style.apply(highlight_near_v20_low, axis=1) if title == "V20 Stocks with All PASS Criteria" else pass_df[display_columns],
                        use_container_width=True
                    )
                    csv = pass_df[display_columns].to_csv(index=False)
                    st.download_button(
                        label=f"Download {title} as CSV",
                        data=csv,
                        file_name=f"{title.lower().replace(' ', '_')}.csv",
                        mime="text/csv",
                        key=f"download_{title.lower().replace(' ', '_')}"
                    )
                    if st.button(f"Copy {title} to Clipboard", key=f"copy_{title.lower().replace(' ', '_')}"):
                        copy_to_clipboard(pass_df[display_columns], title)
                else:
                    st.info(f"No {title.lower()} meet all PASS criteria.")

# Stock input form
st.subheader("Add New Stock Alert")
with st.form(key="add_stock_form"):
    col1, col2 = st.columns(2)
    with col1:
        symbol = st.text_input("Stock Symbol", placeholder="BAJAJHFL.NS", key="symbol_input")
    with col2:
        strategy = st.selectbox("Strategy", strategies, index=strategies.index("V20") if "V20" in strategies else 0, key="strategy_select")
    submit_button = st.form_submit_button("Add Stock")

    if submit_button and symbol:
        conn = sqlite3.connect('stock_alerts.db')
        c = conn.cursor()
        c.execute("SELECT id FROM stocks WHERE symbol = ? AND strategy = ?", (symbol.upper(), strategy))
        existing_stock = c.fetchone()
        
        if existing_stock:
            st.error(f"Stock {symbol.upper()} with strategy '{strategy}' already exists!")
            logging.warning(f"Attempted to add duplicate stock {symbol.upper()} with strategy {strategy}")
            conn.close()
        else:
            current_price, v20_range = get_stock_data(symbol)
            if strategy == "V20":
                if current_price is not None and v20_range:
                    alert_price, target_price = v20_range
                    created_at = int(time.time())
                    c.execute("""INSERT INTO stocks 
                                 (id, symbol, initial_price, alert_price, target_price, strategy, enabled, created_at, 
                                 alert_triggered, last_notified_alert, last_notified_target, notification_cooldown) 
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                             (str(uuid.uuid4()), symbol.upper(), current_price, alert_price, target_price, strategy, 1, 
                              created_at, 0, 0, 0, default_cooldown))
                    conn.commit()
                    st.success(f"Added {symbol.upper()} with V20 alert price ₹{alert_price:.2f} and target price ₹{target_price:.2f}")
                    logging.info(f"Added stock {symbol.upper()} with initial price ₹{current_price:.2f}, alert price ₹{alert_price:.2f}, target price ₹{target_price:.2f}, strategy {strategy}")
                    conn.close()
                else:
                    st.error(f"Invalid symbol {symbol.upper()}: No V20 range data available.")
                    logging.error(f"Failed to add stock {symbol.upper()}: No V20 range data")
                    conn.close()
            else:  # SMA strategy
                try:
                    stock_data = yf.download(symbol.upper(), start=start_date, end=end_date, progress=False)
                    if stock_data.empty:
                        st.error(f"No data found for {symbol.upper()}")
                        conn.close()
                    else:
                        stock_data['SMA_20'] = stock_data['Close'].rolling(window=20).mean()
                        stock_data['SMA_50'] = stock_data['Close'].rolling(window=50).mean()
                        stock_data['SMA_200'] = stock_data['Close'].rolling(window=200).mean()
                        latest_data = stock_data.iloc[-1]
                        sma_20 = float(latest_data['SMA_20'])
                        sma_50 = float(latest_data['SMA_50'])
                        sma_200 = float(latest_data['SMA_200'])
                        if sma_200 > sma_50 and sma_50 > sma_20:
                            created_at = int(time.time())
                            c.execute("""INSERT INTO stocks 
                                         (id, symbol, initial_price, alert_price, target_price, strategy, enabled, created_at, 
                                         alert_triggered, last_notified_alert, last_notified_target, notification_cooldown) 
                                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                     (str(uuid.uuid4()), symbol.upper(), current_price, sma_20, sma_50, strategy, 1, 
                                      created_at, 0, 0, 0, default_cooldown))
                            conn.commit()
                            st.success(f"Added {symbol.upper()} with SMA strategy (20 SMA ₹{sma_20:.2f}, 50 SMA ₹{sma_50:.2f})")
                            logging.info(f"Added stock {symbol.upper()} with initial price ₹{current_price:.2f}, SMA strategy")
                            conn.close()
                        else:
                            st.error(f"{symbol.upper()} does not meet SMA strategy criteria (200 SMA > 50 SMA > 20 SMA)")
                            conn.close()
                except Exception as e:
                    st.error(f"Error processing {symbol.upper()} for SMA strategy: {e}")
                    logging.error(f"Error processing {symbol.upper()} for SMA strategy: {e}")
                    conn.close()

# Display and manage stocks
st.subheader("Current Stock Alerts")
conn = sqlite3.connect('stock_alerts.db')
df = pd.read_sql_query("SELECT * FROM stocks", conn)
conn.close()

if not df.empty:
    for index, row in df.iterrows():
        with st.expander(f"{row['symbol']} - {row['strategy']}"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                if st.button("Delete", key=f"delete_{row['id']}"):
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("DELETE FROM stocks WHERE id = ?", (row['id'],))
                    conn.commit()
                    conn.close()
                    st.rerun()
                    logging.info(f"Deleted stock {row['symbol']}")
            with col2:
                if st.button("Edit", key=f"edit_{row['id']}"):
                    st.session_state[f"edit_mode_{row['id']}"] = True
            with col3:
                if st.button("Disable/Enable", key=f"toggle_{row['id']}"):
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    new_status = 0 if row['enabled'] else 1
                    c.execute("UPDATE stocks SET enabled = ? WHERE id = ?", (new_status, row['id']))
                    conn.commit()
                    conn.close()
                    st.rerun()
                    logging.info(f"{'Enabled' if new_status else 'Disabled'} stock {row['symbol']}")
            with col4:
                st.write(f"Enabled: {'Yes' if row['enabled'] else 'No'}")

            if st.session_state.get(f"edit_mode_{row['id']}", False):
                with st.form(key=f"edit_form_{row['id']}"):
                    ecol1, ecol2 = st.columns(2)
                    with ecol1:
                        new_alert_price = st.number_input("New Alert Price", value=float(row['alert_price']), key=f"alert_{row['id']}")
                    with ecol2:
                        new_target_price = st.number_input("New Target Price", value=float(row['target_price']), key=f"target_{row['id']}")
                    if st.form_submit_button("Save Changes"):
                        conn = sqlite3.connect('stock_alerts.db')
                        c = conn.cursor()
                        c.execute("UPDATE stocks SET alert_price = ?, target_price = ? WHERE id = ?",
                                 (new_alert_price, new_target_price, row['id']))
                        conn.commit()
                        st.session_state[f"edit_mode_{row['id']}"] = False
                        st.rerun()
                        logging.info(f"Updated stock {row['symbol']} with new alert price ₹{new_alert_price:.2f}, target price ₹{new_target_price:.2f}")
                        conn.close()

            st.write(f"Initial Price: ₹{row['initial_price']:.2f}")
            st.write(f"Alert Price: ₹{row['alert_price']:.2f}")
            st.write(f"Target Price: ₹{row['target_price']:.2f}")
            st.write(f"Alert Triggered: {'Yes' if row['alert_triggered'] else 'No'}")
            created_at = datetime.fromtimestamp(row['created_at'], tz=ist).strftime('%Y-%m-%d %H:%M:%S') if row['created_at'] > 0 else 'N/A'
            st.write(f"Created At: {created_at}")
            current_price, _ = get_stock_data(row['symbol'])
            if current_price:
                st.write(f"Current Price: ₹{current_price:.2f}")
                if row['strategy'] == 'V20' and row['alert_price'] > 0:
                    distance_from_v20_low = ((current_price - row['alert_price']) / row['alert_price'] * 100)
                    st.write(f"Distance from V20 Low (%): {distance_from_v20_low:.2f}")
            else:
                st.write("Current Price: Unavailable")

# Price checking logic
def check_prices():
    conn = sqlite3.connect('stock_alerts.db')
    df = pd.read_sql_query("SELECT * FROM stocks WHERE enabled = 1", conn)
    conn.close()

    current_time = time.time()

    for _, row in df.iterrows():
        try:
            current_price, _ = get_stock_data(row['symbol'])
            if current_price is None:
                continue

            # Skip notifications for newly added stocks
            if current_time - row['created_at'] < 60 and row['alert_triggered'] == 0:
                if abs(current_price - row['alert_price']) / row['alert_price'] <= 0.01:
                    logging.info(f"Skipping initial alert notification for {row['symbol']} as price is already at/beyond alert price")
                    continue

            # Check alert price (buy signal)
            if row['alert_price'] > 0 and row['alert_triggered'] == 0:
                last_alert_time = row['last_notified_alert'] if row['last_notified_alert'] > 0 else row['created_at']
                if current_time - last_alert_time >= row['notification_cooldown'] and current_price <= row['alert_price'] + 0.01:
                    message = f"🚨 {row['strategy']} Buy Alert: {row['symbol']} hit buy price ₹{row['alert_price']:.2f}! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET alert_triggered = 1, last_notified_alert = ? WHERE id = ?", (current_time, row['id']))
                    conn.commit()
                    conn.close()
                    logging.info(f"Buy alert triggered for {row['symbol']} at ₹{current_price:.2f}")

            # Check target price (sell signal) only after alert is triggered
            if row['target_price'] > 0 and row['alert_triggered'] == 1:
                last_target_time = row['last_notified_target'] if row['last_notified_target'] > 0 else row['created_at']
                if current_time - last_target_time >= row['notification_cooldown'] and current_price >= row['target_price'] - 0.01:
                    message = f"🎯 {row['strategy']} Sell Alert: {row['symbol']} hit target price ₹{row['target_price']:.2f}! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET last_notified_target = ? WHERE id = ?", (current_time, row['id']))
                    conn.commit()
                    conn.close()
                    logging.info(f"Sell alert triggered for {row['symbol']} at ₹{current_price:.2f}")

        except Exception as e:
            logging.error(f"Error checking {row['symbol']}: {e}")
            st.error(f"Error checking {row['symbol']}: {e}")

# Schedule price checks
def run_scheduler():
    schedule.every(check_interval).minutes.do(check_prices)
    while True:
        schedule.run_pending()
        time.sleep(60)

# Start scheduler in background thread
if st.session_state.scheduler_thread:
    st.session_state.scheduler_thread = False  # Prevent multiple threads
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()