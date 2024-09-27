import requests
from yfinance import Ticker
from datetime import date
from typing import Dict, List, Union
from pprint import pprint
from utils import save_data  # Add this import at the top of the file

pp = pprint.PrettyPrinter(indent=4)

# base url for the API
BASE_API_URL = "https://openlibrary.org/works/"
FILE_FORMAT = ".json"
TICKERS_LIST = [
    "APPL",
    "MSFT",
    "AMZN",
    "META",
    "NTFX",
    "GOOG",
]

def collect_stock_data(stock_ticker: str) -> Dict:
    try:
        # Get the stock data from the API and return a dataframe with the stock data
        # Use the yfinance library to get the stock data with the ticker symbol from the ticker list
        ticker_data = Ticker(stock_ticker)
        df = ticker_data.history(period="1d")
        nrows = df.shape[0]
        
        if nrows == 1:
            return df
        else:
            raise Exception("Data frame is empty or contains more than one row")
    except Exception as e:
        print(e)


for item in TICKERS_LIST:
    response = collect_stock_data(item)
    pp.pprint(response)
    
    # Save the data to the mock data lake
    file_name = f"{date.today().strftime('%Y%m%d')}_stock_{item}"
    save_data(response, file_name, context="stocks", file_type="csv")

