import requests
from datetime import date
from typing import Dict, List, Union
from pprint import pprint
from utils import save_data  # Add this import at the top of the file

# base url for the API
BASE_API_URL = "https://openlibrary.org/works/"
FILE_FORMAT = ".json"
IDS_LIST = [
    "OL47317227M",
    "OL38631342M",
    "OL46057997M",
    "OL26974419M",
    "OL10737970M",
    "OL25642803M",
    "OL20541993M",
]

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


for item in IDS_LIST:
    response_json = collect_single_book_data(BASE_API_URL, item, FILE_FORMAT)
    pprint(response_json) # Print the statement to display results
    # Save the data to the mock data lake
    file_name = f"{date.today().strftime('%Y%m%d')}_book_{item}"
    save_data(response_json, file_name, context="books", file_type="json")

