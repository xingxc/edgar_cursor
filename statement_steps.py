# %%

# imports
import requests
import edgar_functions_working
from bs4 import BeautifulSoup
import utility_belt


# %%

# input varibles
headers = {"User-agent": "email@email.com"}
ticker = "sofi"

# get cik and accession numbers
cik = edgar_functions_working.cik_matching_ticker(ticker, headers=headers)
acc = edgar_functions_working.get_filter_filing(
    ticker, headers=headers, ten_k=False, accession_number_only=True
)

# accession number and accession number date
acc_num = acc.iloc[0].replace("-", "")
acc_date = acc.index[0]


# %% get all statement soup

statement_link_log = edgar_functions_working.get_statement_links(ticker, acc_num, acc_date, headers)
statement_names = list(statement_link_log[acc_date].keys())

name = statement_names[1]
statement_link = statement_link_log[acc_date][name]


statement_soup = edgar_functions_working.get_statement_soup(statement_link, headers)

# %%

(
    columns,
    columns_dict,
    values_set,
    index_dates,
) = edgar_functions_working.extract_columns_values_and_dates_from_statement(statement_soup)

df = edgar_functions_working.create_dataframe_of_statement_values_columns_dates(values_set, columns, index_dates)
df = df.T

# %%
