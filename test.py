import yfinance as yf
import time
try:
    time.sleep(2)  # Simulate delay
    data = yf.download('APARINDS.NS', start='2024-01-01', end='2025-07-22', progress=False)
    print(data)
except Exception as e:
    print(f"Download Error: {str(e)}")