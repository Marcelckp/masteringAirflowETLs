import os
import json
import pandas as pd
from utils import save_data
from datetime import datetime
from argparse import ArgumentParser
# Original
# pp = pprint.PrettyPrinter(indent=4)

# BASE_RAW_JSON_BOOKS_PATH = f"/data_lake/raw/json/books/"

# data = []

# # Iterate over the files in the data lake raw/json/books/ directory 
# # Then we will read each json file and load the content into a dictionary 
# # Then we will extract the open library id from the file name
# # Then we will extract the file creation date from the file name
# # Then we will read the json file and load the content into a dictionary 
# # Then we will create a dictionary with the data we want to save
# # Finally we will save the dictionary to the data lake refined/parquet/books/ directory
# # We will also read the parquet file and print the content to the console
# # This is our process of transforming and cleaning the data
# for file_name in os.listdir(BASE_RAW_JSON_BOOKS_PATH):
#     file_full = f"{BASE_RAW_JSON_BOOKS_PATH}{file_name}"
#     file_creation_date = datetime.strptime(file_name.split("_")[0], "%Y%m%d").strftime(
#         "%Y-%m-%d"
#     )
#     open_library_id = file_name.split("_")[-1].split(".")[0]
#     # Read the json file and load the content into a dictionary 
#     with open(file_full, "r") as fp:
#         item = json.load(fp)
#         d = {
#             "id": open_library_id,
#             "title": item["title"],
#             "subtitle": item["subtitle"] if "subtitle" in item else None,
#             "number_of_pages": item["number_of_pages"]
#             if "number_of_pages" in item
#             else None,
#             "publish_date": item["publish_date"] if "publish_date" in item else None,
#             "publish_country": item["publish_country"]
#             if "publish_country" in item
#             else None,
#             "by_statement": item["by_statement"] if "by_statement" in item else None,
#             "publish_places": "|".join(item["publish_places"])
#             if "publish_places" in item
#             else None,
#             "publishers": "|".join(item["publishers"])
#             if "publishers" in item
#             else None,
#             "authors_uri": "|".join(
#                 [author_dict["key"] for author_dict in item["authors"]]
#             )
#             if "authors" in item
#             else None,
#             "collect_date": file_creation_date,
#         }

#         data.append(d)

# # Save the data to the data lake refined/parquet/books/ directory
# save_data(data, "books", zone="refined", context="books", file_type="parquet")

# # Read the parquet file and print the content to the console to verify the data
# df_parquet = pd.read_parquet("/usercode/data_lake/refined/parquet/books/books.parquet")
# pp.pprint(df_parquet)

if __name__ == "__main__":
    parser = ArgumentParser(description="Parser of book collection")
    parser.add_argument(
        "--data_lake_path", required=True, help="Airflow's data lake path in docker"
    )
    args = parser.parse_args()
    data = []
    BASE_RAW_JSON_BOOKS_PATH = f"{args.data_lake_path}raw/json/books/"
    for file_name in os.listdir(BASE_RAW_JSON_BOOKS_PATH):
        file_full = f"{BASE_RAW_JSON_BOOKS_PATH}{file_name}"
        file_creation_date = datetime.strptime(
            file_name.split("_")[0], "%Y%m%d"
        ).strftime("%Y-%m-%d")
        open_library_id = file_name.split("_")[-1].split(".")[0]
        with open(file_full, "r") as fp:
            item = json.load(fp)
            d = {
                "id": open_library_id,
                "title": item["title"] if "title" in item else None,
                "subtitle": item["subtitle"] if "subtitle" in item else None,
                "number_of_pages": item["number_of_pages"] if "number_of_pages" in item else None,
                "publish_date": item["publish_date"] if "publish_date" in item else None,
                "publish_country": item["publish_country"] if "publish_country" in item else None,
                "by_statement": item["by_statement"] if "by_statement" in item else None,
                "publish_places": "|".join(item["publish_places"]) if "publish_places" in item else None,
                "publishers": "|".join(item["publishers"]) if "publishers" in item else None,
                "authors_uri": "|".join(
                    [author_dict["key"] for author_dict in item["authors"]]
                ) if "authors" in item else None,
                "collect_date": file_creation_date,
            }

            data.append(d)
    save_data(
        data,
        "books",
        zone="refined",
        context="books",
        file_type="parquet",
        base_path=args.data_lake_path,
    )