import pandas as pd
import numpy as np
from pycoingecko import CoinGeckoAPI
from typing import List, Optional, Dict
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

def get_coin_market_cap_dataframe(coin_ids: List[str]) -> pd.DataFrame:
    """
    Extracts market cap ranks for a list of coin IDs using CoinGecko API and returns a DataFrame.

    Args:
        coin_ids: A list of coin IDs.

    Returns:
        A Pandas DataFrame with columns "id" and "market_cap_rank". 
        Ranks are None for coins with errors during data retrieval.
    """

    cg = CoinGeckoAPI()

    coin_data = []

    for coin_id in coin_ids:
        try:
            cg_data = cg.get_coin_by_id(id=coin_id)
            market_cap_rank = cg_data.get("market_cap_rank")
            # Convert to integer (if float)
            market_cap_rank = int(market_cap_rank) if market_cap_rank else None
            print(f"{coin_id}: {market_cap_rank}")
            coin_data.append({"id": coin_id, "market_cap_rank": market_cap_rank})
        except Exception as e:
            logging.error(f"Error retrieving data for {coin_id}: {e}")
            coin_data.append({"id": coin_id, "market_cap_rank": None})

    # Create DataFrame from the data
    df = pd.DataFrame(coin_data)
    df = df.dropna(subset=["market_cap_rank"])  # Drop rows with None in "market_cap_rank"

    # Set ID as index
    df = df.set_index("id")

    return df

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
        prices = cg.get_price(ids=ids_list, vs_currencies="usd", include_market_cap=True)
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
# print(prc_data)

ids_list = prc_data.index.tolist()
# print(ids_list)

mcap_data = get_coin_market_cap_dataframe(coin_ids=ids_list)
#print(mcap_data)

prc_data = prc_data.merge(ids_df, on="id", how="inner")
prc_data = prc_data.merge(mcap_data, on="id", how="inner")

prc_data["symbol"] = prc_data["symbol"].str.upper()

prc_data = prc_data[prc_data["usd_market_cap"] > 0]
prc_data[["symbol", "usd", "usd_market_cap", "market_cap_rank"]].to_csv("prc_data.csv", index=False)

# Usage example
# Assuming you have the required CSV files in the specified paths, you can run the script to generate "prc_data.csv".
