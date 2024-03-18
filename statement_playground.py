# %% USED imports
import utility_belt
import edgar_functions
import matplotlib.pyplot as plt
from edgar_functions import links_logged
from datetime import datetime

# Unused imports
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

headers = {"User-Agent": "email@email.com"}
# ticker = "pypl"
# ticker = "nvda"
ticker = "sofi"
statement_keys_map = utility_belt.import_json_file("statement_key_mapping.json")

# Get CIK and Filing information
cik = edgar_functions.cik_matching_ticker(ticker, headers)
filings_df = edgar_functions.get_submissions_for_ticker(
    ticker, headers, only_filings_df=True
)

# %% # Get the accession number for the 10-K filing

acc_10q = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=False, accession_number_only=True
)


# %% Return the baselink for the filing

# convert to datetime object
statement_date = datetime.strptime(acc_10q.index[0], "%Y-%m-%d")

# accession number
acc_num = acc_10q.iloc[0].replace("-", "")

# return the baselink
baselink = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_num}/"


# %% Get the statement soup

statement_balance_sheet = "balance_sheet"
statement_income_statement = "income_statement"
statement_cash_flow_statement = "cash_flow_statement"


# %% Return the statements

statement_dict = {
    "balance_sheet": None,
    "income_statement": None,
    "cash_flow_statement": None,
    "income_comprehensive": None,
}

for k in statement_dict.keys():
    statement_dict[k] = edgar_functions.process_one_statement(
        ticker, acc_num, k, statement_keys_map, headers
    )

links_logged
df_balance, columns_balance = statement_dict["balance_sheet"]
df_cash, columns_cash = statement_dict["cash_flow_statement"]
df_income, columns_income = statement_dict["income_statement"]
df_income_comprehensive, columns_comprehensive = statement_dict["income_comprehensive"]

display(df_balance) 
display(df_cash)
display(df_income)
display(df_income_comprehensive)


# %%


# income_keys_map = {
#     "ebit": "us-gaap_OperatingIncomeLoss",
#     "income before income taxes": "us-gaap_IncomeLossAttributableToParent",
#     "income tax expense": "us-gaap_IncomeTaxExpenseBenefit",
#     "net operating income": "us-gaap_NetIncomeLoss",
#     "net operating cash": "us-gaap_NetCashProvidedByUsedInOperatingActivities",
# }

# print(df_income.loc[income_keys_map["ebit"]])
# print(df_income.loc[income_keys_map["income before income taxes"]])
# print(df_income.loc[income_keys_map["income tax expense"]])
# print(df_income.loc[income_keys_map["net operating income"]])
# print(df_cash.loc[income_keys_map["net operating cash"]])


# income_before_tax = df_income.loc[income_keys_map["income before income taxes"]]
# tax_expense = df_income.loc[income_keys_map["income tax expense"]]

# tax_expense / income_before_tax
# %%
[
    # "us-gaap_IncomeStatementAbstract",
    # "us-gaap_Revenues",
    # "us-gaap_CostOfRevenue",
    # "us-gaap_GrossProfit",
    # "us-gaap_ResearchAndDevelopmentExpense",
    # "us-gaap_SellingGeneralAndAdministrativeExpense",
    # "nvda_BusinessCombinationAdvancedConsiderationWrittenOff",
    # "us-gaap_OperatingExpenses",
    # "us-gaap_OperatingIncomeLoss",
    # "us-gaap_InvestmentIncomeInterest",
    # "us-gaap_InterestExpense",
    # "us-gaap_OtherNonoperatingIncomeExpense",
    # "us-gaap_NonoperatingIncomeExpense",
    # "us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    # "us-gaap_IncomeTaxExpenseBenefit",
    # "us-gaap_NetIncomeLoss",
    # "us-gaap_EarningsPerShareBasic",
    # "us-gaap_EarningsPerShareDiluted",
    # "us-gaap_WeightedAverageNumberOfSharesOutstandingBasic",
    # "us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding",
]
