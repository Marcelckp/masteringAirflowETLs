import requests
import pandas as pd
from datetime import date
from typing import Dict, List, Union
from pprint import pprint
from utils import save_data
from argparse import ArgumentParser

# base url for the API
BASE_API_URL = "https://openlibrary.org/works/"
FILE_FORMAT = ".json"

# These will now be passed as params from the DAG
# IDS_LIST = [
#     "OL47317227M",
#     "OL38631342M",
#     "OL46057997M",
#     "OL26974419M",
#     "OL10737970M",
#     "OL25642803M",
#     "OL20541993M",
# ]

def collect_single_book_data(base_url: str, open_library_id: str, file_format: str) -> Dict:
    url = f"{base_url}{open_library_id}{file_format}"
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
        else:
            msg = (
                f"and error occurred in the request. It returned code: {r.status_code}"
            )
            raise Exception(msg)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--data_lake_path', required=True, help="Airflow's data lake path in docker container")
    parser.add_argument('--open_library_ids', required=True, help="Comma-separated list of open library ids")
    parser.add_argument('--execution_date', required=True, help="Execution date of the Airflow DAG")
    args = parser.parse_args()

    ID_LIST = args.open_library_ids.split(',')
    
    # Block list books to exclude them from the collection
    blocklist = []

    try:
        df = pd.read_parquet(f"{args.data_lake_path}refined/books/books.parquet")

        df["collect_data"] = pd.to_datetime(df["collect_date"])

        # We use the execution date of the DAG to block books that were collected more than 12 months ago
        df["execution_date"] = args.execution_date
        df["execution_date"] = pd.to_datetime(df["execution_date"])
        df["diff_months"] = df["execution_date"].dt.to_period("M").astype(int) - df[
            "collect_date"
        ].dt.to_period("M").astype(int)

        blocklist = df.query("diff_months > 12")["id"].values.tolist()

        blocklist = df[df["publish_date"].isna()]["id"].tolist()
    except FileNotFoundError as e:
        print(e)
        pass

    # Remove the books that are in the block list from the ID list
    cleaned_ids_list = list(set(ID_LIST) - set(blocklist))

    for item in ID_LIST:
        # Remove the leading and trailing whitespace from the item and remove the comma if it exists
        item = item.strip()

        response_json = collect_single_book_data(BASE_API_URL, item, FILE_FORMAT)
        pprint(response_json) # Print the statement to display results
        
        # Save the data to the mock data lake
        file_name = f"{date.today().strftime('%Y%m%d')}_book_{item}"
        save_data(
            response_json, 
            file_name, 
            context="books", 
            file_type="json",
            base_path=args.data_lake_path
        )