# %% Begin
# import modules
import datetime
import requests
import json
import subprocess
import csv

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import parser_edgar
import cursor_edgar

# Create the EDGAR cursor object
ticker = "AAPL"
ticker = "SOFI"
# ticker = "SOFI"
cursor = cursor_edgar.CursorEdgar()

# Return the ticker CIK
df_tickers = cursor.get_df_tickers()
cursor.get_cik(ticker)

# Return the Concept keys
cursor.query_company_facts()
keys_concept = cursor.get_keys_concept()

std_input = f"mkdir {ticker}"
subprocess.run(std_input, shell=True)


with open(f"{ticker}/{ticker}_key_concept.csv", "w") as file:
    wr = csv.writer(file, quoting=csv.QUOTE_ALL)
    wr.writerow(keys_concept)

# %%

key_concept = "Assets"
df = cursor.query_get_df_concept(key_concept)

df = df.drop_duplicates(subset=["end", "val"], keep="last")

df["end"] = pd.to_datetime(df["end"], format=r"%Y-%m-%d")


df_10q = df[df["form"] == "10-Q"]
df_10k = df[df["form"] == "10-K"]


fig, ax = plt.subplots(1, 1, figsize=(12, 8))
ax.scatter(df["end"], df["val"], marker="o")
# ax.plot(df_10q["end"], df_10q["val"], marker="o")
# ax.plot(df_10k["end"], df_10k["val"], marker="o")
ax.tick_params(axis="x", rotation=45)
ax.grid(linestyle = '--')
# %%



