import pandas as pd
import json
import os
from typing import Dict, List, Union

# NOTE: A dataframe is a table with rows and columns that can be used to store data in a structured format that mimics a table in a database.

# Save data to the data lake 
def save_data(
    file_content: Union[List[Dict], Dict, List, str],
    file_name: str,
    zone: str = "raw",
    context: str = "books",
    file_type: str = "csv",
) -> None:
    # Define the base path for the data lake where the specific data will be saved
    DATA_LAKE_BASE_PATH = f"/data_lake/{zone}/{file_type}/{context}/"

    # Define the full file name with the date and the file name
    full_file_name = f"{DATA_LAKE_BASE_PATH}{file_name}"

    # Saving API Data from the Books API
    # Save the data to the data lake for json files
    if file_type == "json" and zone == "raw":
        with open(f"{full_file_name}.json", "w") as fp:
            json.dump(file_content, fp)

    # Saving API Data from the Stocks API
    elif file_type == "csv" and zone == "raw": # NOTE: This is for the stocks API data as the data is different in format from the books API data
        if not isinstance(file_content, pd.DataFrame):
            df = pd.DataFrame(file_content)
        else:
            df = file_content
        df.to_csv(f"{full_file_name}.csv", index=False)

    # Save the data to the data lake for parquet files (refined books and stocks data respectively)
    elif file_type == "parquet" and zone == "refined":
        if not isinstance(file_content, pd.DataFrame):
            df = pd.DataFrame(file_content)
        else:
            df = file_content

        cols_except_dt = [c for c in df.columns.tolist() if c != "collect_date"]
        df = df.sort_values("collect_date", ascending=False).drop_duplicates(
            subset=cols_except_dt, keep="last"
        )
        df.to_parquet(f"{full_file_name}.parquet")

    else:
        print(
            "Specified file type not found or combination of Zone and File Type does not match"
        )