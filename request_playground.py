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
# ticker = "AAPL"
# ticker = "SOFI"
ticker = "SAND"
cursor = cursor_edgar.CursorEdgar()

# Return the ticker CIK
df_tickers = cursor.get_df_tickers()
cursor.get_cik(ticker)

# Return the Concept keys
cursor.query_company_facts()

cursor.company_facts

keys_concept = cursor.get_keys_concept()

std_input  =f"mkdir {ticker}"
subprocess.run(std_input, shell=True)


with open(f'{ticker}/{ticker}_key_concept.csv', 'w') as file:
    wr = csv.writer(file, quoting=csv.QUOTE_ALL)
    wr.writerow(keys_concept)



cursor_edgar.company_facts.json()

# %% Define plotting

def get_concept_df(cursor_edgar, key_concept, mask_form=None):
    cursor_edgar.query_get_df_concept(key_concept)

    df = cursor_edgar.df_concept[key_concept]
    if mask_form is not None:
        df = df[df["form"] == mask_form]
    df = df.drop_duplicates(subset=["end", "val"], keep="last")

    return df

# %%

# key_concept = "GrossProfit"
key_concept = "Assets"
# key_concept = "OperatingExpenses"

# df_raw = get_concept_df(cursor, key_concept=key_concept, mask_form=None)


cursor_edgar.query_get_df_concept(key_concept)

#%%

df_raw['end']




#%%





# Convert str to date time and get duration
# df_raw["start"] = pd.to_datetime(df_raw["start"], format=r"%Y-%m-%d")
# df_raw["end"] = pd.to_datetime(df_raw["end"], format=r"%Y-%m-%d")
# df_raw["duration"] = df_raw["end"] - df_raw["start"]

# duration_quarter = (df_raw["duration"] > datetime.timedelta(days=70)) & (
#     df_raw["duration"] < datetime.timedelta(days=110)
# )
# duration_annual = df_raw["duration"] > datetime.timedelta(days=340)

# df_FY = df_raw[duration_annual]
# df_Q = df_raw[duration_quarter]


# df_Q["start_ym"] = df_Q["start"].dt.strftime("%Y-%m")
# df_Q["end_ym"] = df_Q["end"].dt.strftime("%Y-%m")


# df_Q.drop_duplicates(subset=["start_ym", "end_ym"], keep="last")


# %%

# set(df_Q["start"].dt.month)

quarters_dict = {}

quarter_start = {
    "Q1": [12, 1],
    "Q2": [3, 4],
    "Q3": [6, 7,8],
    "Q4": [9, 10],
}

for quarter, months in quarter_start.items():

    mask_index = []
    for k,v in df_Q['start'].dt.month.items():
        if v in months:
            mask_index.append(k)

    quarters_dict[quarter] = df_Q.loc[mask_index]
    quarters_dict[quarter] = quarters_dict[quarter].reset_index()

    print(quarter)
    display(quarters_dict[quarter])



# quarter_end = {
#     "Q1": [3, 4],
#     "Q2": [6, 7],
#     "Q3": [9, 10],
#     "Q4": [12, 1],
# }

# display(df_FY)


# years_FY = set(pd.DatetimeIndex(df_FY["start"]).year)
# years_FY = set(pd.DatetimeIndex(df_FY["start"]).year)


# %%

quarters_dict, df_history = parser_edgar.accumulate_data_parsing(df_10k)


fig, ax = plt.subplots(1, 1, figsize=(12, 8))
ax.plot(df_history["end"], df_history["val"] / 10**6, marker="o")
ax.set_title(f"{key_concept}")
ax.set_ylabel("Dollar value $ (millions)")
ax.set_xlabel("Date")
ax.grid(linestyle="--")
plt.xticks(rotation=60)


# %% CONCEPTS CHECK


def plot_concept(cursor_edgar, key_concept, mask):
    cursor_edgar.query_get_df_concept(key_concept)

    df = cursor_edgar.df_concept[key_concept]
    df = df[df["form"] == mask]

    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    ax.plot(df["end"], df["val"] / 10**6, marker="o")
    ax.set_title(f"{key_concept}")
    ax.set_ylabel("Dollar value $ (millions)")
    ax.set_xlabel("Date")
    ax.grid(linestyle="--")
    plt.xticks(rotation=60)


# %% ####-------------- ASSETS --------------####

# Assets
plot_concept(cursor, key_concept="Assets", mask="10-Q")
# Cash and cash equivalents
plot_concept(cursor, key_concept="CashAndCashEquivalentsAtCarryingValue", mask="10-Q")

# %% ####-------------- REVENUES --------------####

# Revenues
plot_concept(cursor, key_concept="Revenues", mask="10-Q")
# # Sales revenue net
# plot_concept(cursor, key_concept="SalesRevenueNet", mask="10-Q")
# Sales revenue services gross
# plot_concept(cursor, key_concept="SalesRevenueServicesGross", mask="10-Q")


# %% ####-------------- PROFITS --------------####

# Gross Profits
plot_concept(cursor, key_concept="GrossProfit", mask="10-Q")

# %% ####-------------- EXPENSES --------------####

# Operating Expenses
plot_concept(cursor, key_concept="OperatingExpenses", mask="10-Q")
# Marketing Expenses
plot_concept(cursor, key_concept="NonoperatingIncomeExpense", mask="10-Q")
# Marketing Expenses
# plot_concept(cursor, key_concept="MarketingExpense", mask="10-Q")


# %% Get these parameters

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
