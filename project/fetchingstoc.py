import yfinance as yf

# Example: Download Apple stock data
stock = yf.download("RELIANCE.NS", start="2020-01-01", end="2025-12-31")
print(stock.tail(180))