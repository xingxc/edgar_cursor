# %%

########## ------ START OF SCRIPT ------ ##########

# Imports
import os
import requests
import subprocess
import utility_belt
import pandas as pd
import edgar_functions


# %%


def get_statement_df(statement_link, headers):
    """
    args:
        - statement_link[str]: link to the statement
        - headers[dict]: headers for the request

    returns:
        - df[pd.DataFrame]: dataframe of the statement

    description:
        - retrieves the statement from the link and returns it as a dataframe
    """

    statement_soup = edgar_functions.get_statement_soup(statement_link, headers)

    (
        columns,
        columns_dict,
        values_set,
        index_dates,
    ) = edgar_functions.extract_columns_values_and_dates_from_statement(statement_soup)

    df = edgar_functions.create_dataframe_of_statement_values_columns_dates(
        values_set, columns, index_dates
    )
    df = df.T
    return df


# %% Inputs and initilizing info

headers = {"User-agent": "email@email.com"}

path_dict = {
    "ticker": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker",
    "json": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/statement_key_mapping.json",
}
ticker = "nvda"
path_dict["ticker"] = os.path.join(path_dict["ticker"], ticker.lower())
utility_belt.mkdir(path_dict["ticker"])


# %% Get 10k and 10q accession numbers:

acc_10k = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=True, accession_number_only=False
)
acc_10q = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=False, accession_number_only=False
)

df_acc = pd.concat([acc_10k, acc_10q], axis=0)
df_acc.sort_index(inplace=True)


# %% Retrieve links for all statements

links_full = {}
links_core = {}

for acc_date, row in df_acc.iterrows():
    acc_num = row["accessionNumber"]

    links_statement = edgar_functions.get_statement_links(
        ticker,
        acc_num,
        acc_date,
        headers,
    )

    links_full[acc_date] = links_statement[acc_date]
    print(f"{acc_date} links retrieved")

# % Filter for core links
for acc_date, links in links_full.items():
    links_core[acc_date] = edgar_functions.filter_links(links, path_dict["json"])

# %% Export accession numbers and links to ticker folder

utility_belt.export_json_file(
    os.path.join(path_dict["ticker"], f"{ticker}_links_full.json"), links_full
)
utility_belt.export_json_file(
    os.path.join(path_dict["ticker"], f"{ticker}_links_core.json"), links_core
)
df_acc.to_csv(os.path.join(path_dict["ticker"], "accession_numbers.csv"))


########## ------ END OF SCRIPT ------ ##########


# %%
# acc_date = df_acc.index[-1]
# print(links_full[acc_date])
# print(links_core[acc_date])

# # %%


# # %%


# # %%

# list(df_acc.index)


# # %%
# _ = list(df_acc.index)

# for i, acc_date in enumerate(_):
#     _[i] = pd.to_datetime(acc_date, format="%Y-%m-%d")

# # %%


# # %%

# # for statement_name, possible_keys_list in statement_map.items():
# #     for possible_key in possible_keys_list:
# #         statement_link = links_dict.get(possible_key, None)
# #         if statement_link is not None:
# #             # links_core[acc_date][statement_name] = statement_link
# #             print(f"{acc_date} - {statement_name} - {statement_link}")


# # links_core = {}

# # for acc_date, links in links_full.items():
# #     links_core[acc_date] = {}


# # %% Filter links for core statements

# acc_date = df_acc.index[-1]
# acc_num = df_acc.loc[acc_date]
# statement_df_dict = {}


# links_statement = edgar_functions.get_statement_links(
#     ticker,
#     acc_num["accessionNumber"],
#     acc_date,
#     headers,
# )

# links_full = {}


# links_core = {}
# df_core = {}

# links_full[acc_date] = links_statement
# links_core[acc_date] = {}
# df_core[acc_date] = {}

# for statement_name, statement_name_list in statement_map.items():
#     for possible_key in statement_name_list:
#         statement_link = links_statement[acc_date].get(possible_key, None)
#         if statement_link is not None:
#             links_core[acc_date][statement_name] = statement_link
#             print(f"{acc_date} - {statement_name} - {statement_link}")

# %%


# %%

# for statement_name, statement_name_list in statement_map.items():
#     for possible_key in statement_name_list:
#         statement_link = links_statement[acc_date].get(possible_key, None)
#         if statement_link is not None:
#             links_core[acc_date][statement_name] = statement_link
#             print(f"{acc_date} - {statement_name} - {statement_link}")


# %% Get the dataframe of the statements

# for name, link in links_core[acc_date].items():
#     df_core[acc_date][name] = get_statement_df(link, headers)


# display(df_core[acc_date]["income_statement"])
# display(df_core[acc_date]["income_comprehensive"])
# display(df_core[acc_date]["balance_sheet"])
# display(df_core[acc_date]["balance_parenthetical"])
# display(df_core[acc_date]["cash_flow_statement"])

# %%
