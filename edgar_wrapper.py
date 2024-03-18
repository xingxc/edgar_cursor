# %%
# Imports
import os
import requests
import subprocess
import utility_belt
import pandas as pd
import edgar_functions_working


def get_statement_df(statement_link, headers):

    statement_soup = edgar_functions_working.get_statement_soup(statement_link, headers)

    (
        columns,
        columns_dict,
        values_set,
        index_dates,
    ) = edgar_functions_working.extract_columns_values_and_dates_from_statement(
        statement_soup
    )

    df = edgar_functions_working.create_dataframe_of_statement_values_columns_dates(
        values_set, columns, index_dates
    )
    df = df.T
    return df


class ticker_statement:
    def __init__(self, ticker, headers):
        self.ticker = ticker
        self.headers = headers
        self.log_links = {}
        self.accession_numbers = None

        self.cik = edgar_functions_working.cik_matching_ticker(ticker, headers=headers)


# %%

headers = {"User-agent": "email@email.com"}

path_dict = {
    "ticker": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker",
    "json": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/statement_key_mapping.json",
}

statement_map = utility_belt.import_json_file(path_dict["json"])
ticker = "sofi"
SOFI = ticker_statement(ticker, headers)

# %% Get 10k and 10q accession numbers:

acc_10k = edgar_functions_working.get_filter_filing(
    ticker, headers=headers, ten_k=True, accession_number_only=False
)
acc_10q = edgar_functions_working.get_filter_filing(
    ticker, headers=headers, ten_k=False, accession_number_only=False
)

df_acc = pd.concat([acc_10k, acc_10q], axis=0)
df_acc.sort_index(inplace=True)


# %% Filter links for core statements

acc_date = df_acc.index[-1]
acc_num = df_acc.loc[acc_date]
statement_df_dict = {}


statement_links = edgar_functions_working.get_statement_links(
    ticker,
    acc_num["accessionNumber"],
    acc_date,
    headers,
)
core_links = {}
core_df = {}
core_links[acc_date] = {}
core_df[acc_date] = {}

for statement_name, statement_name_list in statement_map.items():
    for possible_key in statement_name_list:
        statement_link = statement_links[acc_date].get(possible_key, None)
        if statement_link is not None:
            core_links[acc_date][statement_name] = statement_link
            print(f"{statement_name} - {statement_link}")


# %% Get the dataframe of the statements

core_df[acc_date]["balance_sheet"] = get_statement_df(
    core_links[acc_date]["balance_sheet"], headers
)
core_df[acc_date]["income_statement"] = get_statement_df(
    core_links[acc_date]["income_statement"], headers
)
core_df[acc_date]["cash_flow_statement"] = get_statement_df(
    core_links[acc_date]["cash_flow_statement"], headers
)

display(core_df[acc_date]["balance_sheet"])
display(core_df[acc_date]["income_statement"])
display(core_df[acc_date]["cash_flow_statement"])


