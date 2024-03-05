import pandas as pd
import numpy as np
from pycoingecko import CoinGeckoAPI
import logging

logging.basicConfig(level=logging.ERROR)

# csv with token ids
gitpath = "https://raw.githubusercontent.com/ereboreum/important-docs/main/"
csv_file_name = "indata.csv"
ids_csv_file_name = "symbol-ids.csv"

def read_csv(file_path: str) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the data from the CSV file.
    """
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        raise

def get_prc_mcap(ids_list: list) -> pd.DataFrame:
    """
    Get price and market cap.

    Args:
        ids_list (list): List of token ids.

    Returns:
        pd.DataFrame: DataFrame containing price and market cap data.
    """
    # CoinGecko API
    cg = CoinGeckoAPI()

    try:
        # get current price and mcap
        prices = cg.get_price(ids=ids_list, vs_currencies="usd", include_market_cap="true")
        prices_df = pd.DataFrame.from_dict(prices, orient="index")
        prices_df.index.names = ["id"]

        return prices_df

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return pd.DataFrame()

df = read_csv(gitpath + csv_file_name)
df["symbol"] = df["symbol"].str.lower()

ids_df = read_csv(gitpath + ids_csv_file_name)

merged_df = df.merge(ids_df, on="symbol", how="inner")
ids_list = merged_df["id"].tolist()

prc_data = get_prc_mcap(ids_list=ids_list)
prc_data = prc_data.merge(ids_df, on="id", how="inner")
prc_data["symbol"] = prc_data["symbol"].str.upper()

prc_data = prc_data[prc_data["usd_market_cap"] > 0]
prc_data[["symbol", "usd", "usd_market_cap"]].to_csv("prc_data.csv", index=False)

# Usage example
# Assuming you have the required CSV files in the specified paths, you can run the script to generate 'prc_data.csv'.
