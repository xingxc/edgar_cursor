# %%
import edgar_functions
from edgar_functions import links_logged
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import utility_belt

headers = {"User-Agent": "email@email.com"}
# ticker = "pypl"
ticker = "nvda"
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
# acc_10k = edgar_functions.get_filter_filing(
#     ticker, headers=headers, ten_k=True, accession_number_only=True
# )

# %%

statement_date = datetime.strptime(
    acc_10q.index[0], "%Y-%m-%d"
)  # convert to datetime object
acc_num = acc_10q.iloc[0].replace("-", "")  # accession number
baselink = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_num}/"


# %%

statement_balance_sheet = "balance_sheet"
statement_income_statement = "income_statement"
statement_cash_flow_statement = "cash_flow_statement"

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

# %% CASH FLOWS

links_logged
df_balance = statement_dict["balance_sheet"]
df_cash = statement_dict["cash_flow_statement"]
df_income = statement_dict["income_statement"]
df_income_comprehensive = statement_dict["income_comprehensive"]

# %%

keys_income = utility_belt.import_json_file("ticker_keys/nvda_keys.json")


#%%

for index_value in df_income.index:
    print(index_value)
    for key, value_list in keys_income.items():
        for value in value_list["keys"]:
            if index_value in value:
                df_income.rename(index={index_value: key}, inplace=True)

#%%






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
