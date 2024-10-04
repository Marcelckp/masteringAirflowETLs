import pandas as pd
from argparse import ArgumentParser

# function to display the data in the data lake .parquet file
def display_data_lake_info(data_lake_path: str):
    # display the data in the data lake .parquet file
    df = pd.read_parquet(data_lake_path)
    print(df.head())

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--data_lake_path', required=True, help="Airflow's data lake path in docker container")
    args = parser.parse_args()
    display_data_lake_info(args.data_lake_path)