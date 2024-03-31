# %%
# Imports
import os
import requests
import subprocess
import utility_belt
import pandas as pd
import edgar_functions


class ticker_statement:
    def __init__(self, ticker, headers):
        self.ticker = ticker
        self.headers = headers

        self.cik = edgar_functions.cik_matching_ticker(ticker, headers=headers)


# %% Inputs and initilizing info

headers = {"User-agent": "email@email.com"}
ticker = "nvda"

path_dict = {
    "ticker": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker",
    "json": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/statement_key_mapping.json",
}
path_dict["ticker"] = os.path.join(path_dict["ticker"], ticker.lower())
path_dict["statements"] = os.path.join(path_dict["ticker"], "statements")


# Get link paths and accession dataframe
command = f"find '{path_dict['ticker']}' -type f \( -regex '.*.json' -o -regex '.*.csv' \) -maxdepth 1"
std_out = subprocess.run(command, shell=True, text=True, capture_output=True)
std_out = std_out.stdout.splitlines()

for link in std_out:

    # Get the base name of the link
    base_name = subprocess.run(
        f"basename '{link}' | rev | cut -d. -f2- | rev",
        shell=True,
        text=True,
        capture_output=True,
    ).stdout.strip()

    # Add the link to the path_dict
    path_dict[base_name] = link


# %%

# Get link paths and accession dataframe
command = f"find '{path_dict['statements']}' -type f -regex '.*.csv' -maxdepth 1"
std_out = subprocess.run(command, shell=True, text=True, capture_output=True)
paths_df = std_out.stdout.splitlines()


display(paths_df)
