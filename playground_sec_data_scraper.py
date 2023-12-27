# %%
import pandas as pd
import requests
import bs4 as bs
import logging
import calendar
import numpy as np
import edgar_functions
from edgar_functions import statement_keys_map


headers = {"User-agent": "email@email.com"}
ticker = "pypl"

acc = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=True, accession_number_only=True
)
acc_num = acc.iloc[0].replace("-", "")

## -------- Begin Scraping -------- ##


# Get statement file names

statement_balance_sheet = "balance_sheet"
statement_income_statement = "income_statement"
statement_cash_flow_statement = "cash_flow_statement"

soup_balance_sheet = edgar_functions.get_statement_soup(
    ticker, acc_num, statement_balance_sheet, headers, statement_keys_map
)

soup_income_statement = edgar_functions.get_statement_soup(
    ticker, acc_num, statement_income_statement, headers, statement_keys_map
)

soup_cash_flow_statement = edgar_functions.get_statement_soup(
    ticker, acc_num, statement_cash_flow_statement, headers, statement_keys_map
)
edgar_functions.links_logged

# %%


def standardize_date(date: str) -> str:
    """
    Standardizes date strings by replacing abbreviations with full month names.

    Args:
        date (str): The date string to be standardized.

    Returns:
        str: The standardized date string.
    """
    for abbr, full in zip(calendar.month_abbr[1:], calendar.month_name[1:]):
        date = date.replace(abbr, full)
    return date


columns = []
values_set = []

table_headers = soup_cash_flow_statement.find_all("th", {"class": "th"})
dates = [str(th.div.string) for th in table_headers if th.div and th.div.string]
dates = [standardize_date(date).replace(".", "") for date in dates]
index_dates = pd.to_datetime(dates)

# %%

table_find_all = soup_cash_flow_statement.find_all("table")

unit_multiplier = 1
special_case = False


table_header = table.find("th")
