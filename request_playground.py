# %% Begin

# import modules
import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import cursor_edgar

# Create the EDGAR cursor object
ticker = "TSLA"
cursor = cursor_edgar.CursorEdgar()

# Return the ticker CIK
df_tickers = cursor.get_df_tickers()
cursor.get_cik(ticker)

# Return the Concept keys
cursor.query_company_facts()
keys_concept = cursor.get_keys_concept()

#%% Get the Assets dataframe

key_concept = "Assets"
cursor.query_get_df_concept(key_concept)
df_10Q = cursor.df_concept[key_concept]
df_10Q = df_10Q[df_10Q["form"] == "10-Q"]
fig, ax = plt.subplots(1, 1, figsize=(12, 8))
ax.plot(df_10Q["end"], df_10Q["val"], marker = 'o')
plt.xticks(rotation=60)

#%% Get the Revenue dataframe

key_concept = "Revenues"
cursor.query_get_df_concept(key_concept)
df_10Q = cursor.df_concept[key_concept]
df_10Q = df_10Q[df_10Q["form"] == "10-Q"]
fig, ax = plt.subplots(1, 1, figsize=(12, 8))
ax.plot(df_10Q["end"], df_10Q["val"], marker = 'o')
plt.xticks(rotation=60)

#%% Revenue



#%% Get these parameters

[
    "Sales",
    "Net Sales",
    "Total Revenuue",
    "Cost of Sales",
    "Gross income",
    "SG&A Expenses",
    "Sales and Marketing Expenses",
    "General & Admin Expense",
    "Research and Development",
    "EBITDA",
    "EBITDA Non-GAAP",
    "EBITDA GAAP",
    "Depr & Amount",
    "Operating Income",
    "EBITA," "Operating Income - Non GAAP",
    "Operating Income - GAAP",
    "Interest Expense",
    "Pretax Income",
    "Pretax Income - Non GAAP",
    "Pretax Income - GAAP",
    "Tax Expense",
    "Net Income",
    "Net Income - Non GAAP",
    "Net Income - GAAP",
]
