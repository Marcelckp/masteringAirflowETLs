import pandas as pd
import os
from datetime import datetime
from utils import save_data
import re
import pprint

pp = pprint.PrettyPrinter(indent=4)

BASE_RAW_CSV_STOCKS_PATH = f"/data_lake/raw/csv/stocks/"

dfs = []

# Iterate over the files in the data lake raw/csv/stocks/ directory 
# Then we will read each csv file and load the content into a pandas dataframe
# Then we will rename the columns to be all lowercase and remove spaces
# Then we will select the columns we want to keep
# Then we will convert the date column to a datetime object
# Then we will convert the open, high, low, and close columns to float
# Then we will convert the volume column to an integer
# Then we will add the ticker name and the collect date to the dataframe
# Finally we will concatenate all the dataframes into one and save the data to the data lake refined/parquet/stocks/ directory
# This is our process of transforming and cleaning the data
for file_name in os.listdir(BASE_RAW_CSV_STOCKS_PATH):
    file_full = f"{BASE_RAW_CSV_STOCKS_PATH}{file_name}"
    file_creation_date = datetime.strptime(file_name.split("_")[0], "%Y%m%d").strftime(
        "%Y-%m-%d"
    )
    ticker_name = file_name.split("_")[2].split(".")[0]
    df = pd.read_csv(file_full)
    df.columns = [re.sub(r"\s+", "_", x.lower()) for x in df.columns]
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for item in ["open", "high", "low", "close"]:
        df[item] = df[item].astype(float)
    df["volume"] = df["volume"].astype(int)
    df["ticker_name"] = ticker_name
    df["collect_date"] = file_creation_date
    dfs.append(df)

df = pd.concat(dfs)

# Save the data to the data lake refined/parquet/stocks/ directory
save_data(df, "stocks", zone="refined", context="stocks", file_type="parquet")

# Read the parquet file and print the content to the console to check if the data is correctly saved
df_parquet = pd.read_parquet(
    "/data_lake/refined/parquet/stocks/stocks.parquet"
)
pp.pprint(df_parquet)