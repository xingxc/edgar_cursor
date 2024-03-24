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
        self.log_links = {}
        self.accession_numbers = None

        self.cik = edgar_functions.cik_matching_ticker(ticker, headers=headers)


# %% Inputs and initilizing info

headers = {"User-agent": "email@email.com"}

path_dict = {
    "ticker": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker",
    "json": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/statement_key_mapping.json",
}
ticker = "nvda"
path_dict["ticker"] = os.path.join(path_dict["ticker"], ticker.lower())
utility_belt.mkdir(path_dict["ticker"])


#%%

command = f"find '{path_dict['ticker']}'"  # replace with your command
print(command)

# completed_process = subprocess.run(command, shell=True, text=True, capture_output=True)

# print(completed_process.stdout)

# %%
