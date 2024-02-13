# %%
import edgar_functions
from edgar_functions import statement_keys_map, links_logged
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests

headers = {"User-Agent": "email@email.com"}
ticker = "pypl"


cik = edgar_functions.cik_matching_ticker(ticker, headers)


# Get Filing information

filings_df = edgar_functions.get_submissions_for_ticker(
    ticker, headers, only_filings_df=True
)


# %%
# Get the accession number for the 10-K filing
acc_df = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=True, accession_number_only=False
)

acc_series = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=True, accession_number_only=True
)
acc_num = acc_series.iloc[0].replace("-", "")  # accession number


statement_balance_sheet = "balance_sheet"
statement_income_statement = "income_statement"
statement_cash_flow_statement = "cash_flow_statement"

statement_dict = {
    "balance_sheet": None,
    "income_statement": None,
    "cash_flow_statement": None,
}


for k in statement_dict.keys():
    statement_dict[k] = edgar_functions.process_one_statement(
        ticker, acc_num, k, headers
    )

# %% CASH FLOWS

links_logged
df_cash = statement_dict["cash_flow_statement"]
df_income = statement_dict["income_statement"]

income_keys_map = {
    "ebit": "us-gaap_OperatingIncomeLoss",
    "income before income taxes": "us-gaap_IncomeLossAttributableToParent",
    "income tax expense": "us-gaap_IncomeTaxExpenseBenefit",
    "net operating income": "us-gaap_NetIncomeLoss",
    "net operating cash": "us-gaap_NetCashProvidedByUsedInOperatingActivities",
}

print(df_income.loc[income_keys_map["ebit"]])
print(df_income.loc[income_keys_map["income before income taxes"]])
print(df_income.loc[income_keys_map["income tax expense"]])
print(df_income.loc[income_keys_map["net operating income"]])
print(df_cash.loc[income_keys_map["net operating cash"]])



income_before_tax = df_income.loc[income_keys_map["income before income taxes"]]
tax_expense = df_income.loc[income_keys_map["income tax expense"]]

tax_expense/income_before_tax