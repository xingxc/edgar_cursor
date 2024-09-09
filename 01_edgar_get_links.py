########## ------ START OF SCRIPT ------ ##########
# %%
# Imports
import os
import psycopg
import requests
import subprocess
import sqlalchemy
import utility_belt
import pandas as pd
import edgar_functions
import psql_conn
from bs4 import BeautifulSoup

# %% Inputs and initilizing info

headers = {"User-agent": "email@email.com"}
ticker = "nvda"

# %% Get 10k and 10q accession numbers:

acc_10k = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=True, accession_number_only=False
)
acc_10q = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=False, accession_number_only=False
)

df_accession = pd.concat([acc_10k, acc_10q], axis=0)
df_accession.sort_index(inplace=True)

# %% Retrieve links for all statements

links_full = {}
links_core = {}

df_statement_links = pd.DataFrame()

for acc_num, row in df_accession.iterrows():
    acc_date = row["report_date"]
    links_statement_dict, links_statement_df = edgar_functions.get_statement_links(
        ticker,
        acc_num,
        acc_date,
        headers,
    )

    links_full[acc_num] = links_statement_dict[acc_num]
    links_statement_df.insert(loc=1, column='report_date', value=acc_date)

    df_statement_links = pd.concat([df_statement_links, links_statement_df], axis=0)
    print(f"{acc_num} ; {acc_date} ; links retrieved")


# %%
print(df_accession)  # accession number and date relationship
print(df_statement_links)  # statement links and accession number relationship

# %% Return a filtered dataframe for a specific accession number

# df_filtered = df_statement_links[
#     df_statement_links["accession_number"] == "000104581024000124"
# ]

# _ = df_statement_links[df_statement_links["statement_name"].str.contains("segment")]
# _.to_csv("segment_check.csv")
# segment information is R36 for nvidia

# %% Create postgres engine and export accession numbers to postgres database

dialect = "postgresql"
username = os.getenv("DATABASE_USER")
password = os.getenv("DATABASE_PASSWORD")
host = "localhost"
port = "5432"
db_name = "test"

engine = sqlalchemy.create_engine(
    f"{dialect}+psycopg://{username}:{password}@{host}:{port}/{db_name}"
)

# table names and primary/foreign key constraints
ticker_accession_numbers = f"{ticker}_accession_numbers"
ticker_statement_link = f"{ticker}_statement_link"

accession_pk_column = "accession_number"
links_pk_column = "statement_link"

links_fk_column = "accession_number"
links_fk_constraint_name = "fk_accession_number"


# %%
# Drop tables if they exist with cascade
psql_conn.drop_table_if_exists(ticker_accession_numbers, engine, cascade=True)
psql_conn.drop_table_if_exists(ticker_statement_link, engine, cascade=True)


# %%
# Create tables from dataframes
print(f"export to database: {ticker_accession_numbers}")
df_accession.to_sql(
    ticker_accession_numbers,
    engine,
    if_exists="replace",
    index=True,
)

print(f"export to database: {ticker_statement_link}")
df_statement_links.to_sql(
    ticker_statement_link,
    engine,
    if_exists="replace",
    dtype={},
    index=False,
)

# Add primary and foreign keys
psql_conn.add_primary_key_if_not_exists(
    ticker_accession_numbers,
    accession_pk_column,
    engine,
)

psql_conn.add_primary_key_if_not_exists(
    ticker_statement_link,
    links_pk_column,
    engine,
)

psql_conn.add_foreign_key_if_not_exists(
    ticker_statement_link,
    links_fk_column,
    links_fk_constraint_name,
    ticker_accession_numbers,
    accession_pk_column,
    engine,
)

# %% Export the statement key mapping to the database

path_json = r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/statement_key_mapping.json"
df_json = utility_belt.import_json_file(path_json)
df_json = pd.DataFrame.from_dict(df_json, orient="index").T

df_json.to_sql(
    "statement_key_mapping",
    engine,
    if_exists="replace",
    dtype={},
    index=False,
)

########## ------ END OF SCRIPT ------ ##########


# %% Export accession numbers and links to ticker folder

# path_dict = {
#     "ticker": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker",
#     "json": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/statement_key_mapping.json",
# }

# path_dict["ticker"] = os.path.join(path_dict["ticker"], ticker.lower())
# utility_belt.mkdir(path_dict["ticker"])

# # % Filter for core links
# for acc_num, links in links_full.items():
#     print(f"{acc_num}")
#     links_core[acc_num], _ = edgar_functions.filter_links(links, path_dict["json"])

# # Export links and accession numbers
# utility_belt.export_json_file(
#     os.path.join(path_dict["ticker"], f"{ticker}_links_full.json"), links_full
# )
# utility_belt.export_json_file(
#     os.path.join(path_dict["ticker"], f"{ticker}_links_core.json"), links_core
# )
# df_accession.to_csv(os.path.join(path_dict["ticker"], "accession_numbers.csv"))
