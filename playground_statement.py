# %%
import edgar_functions
from edgar_functions import statement_keys_map, links_logged
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

headers = {"User-Agent": "email@email.com"}
ticker = "pypl"

# Get the accession number for the 10-K filing

cik= edgar_functions.cik_matching_ticker(ticker, headers)

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

soup_balance_sheet = edgar_functions.get_statement_soup(
    ticker, acc_num, statement_balance_sheet, headers, statement_keys_map
)

soup_income_statement = edgar_functions.get_statement_soup(
    ticker, acc_num, statement_income_statement, headers, statement_keys_map
)

soup_cash_flow_statement = edgar_functions.get_statement_soup(
    ticker, acc_num, statement_cash_flow_statement, headers, statement_keys_map
)


# %%



display(cik)

display(links_logged)





# %%
