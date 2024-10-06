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
path_tickers = "/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker"
ticker = "nvda"


# %% Create ticker folder and subfolders
path_ticker = os.path.join(path_tickers, ticker.lower())
path_statement_html = os.path.join(path_ticker, "statements_html")
path_statement_df = os.path.join(path_ticker, "statements_df")

utility_belt.mkdir(path_ticker)
utility_belt.mkdir(path_statement_html)
utility_belt.mkdir(path_statement_df)

# %% Get 10k and 10q accession numbers:

acc_10k = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=True, accession_number_only=False
)
acc_10q = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=False, accession_number_only=False
)

acc_10k['form'] = acc_10k['form'].str.replace('-', '')
acc_10q['form'] = acc_10q['form'].str.replace('-', '')


df_accession = pd.concat([acc_10k, acc_10q], axis=0)
df_accession.sort_index(inplace=True)
df_accession["ixbrl_link"] = None
df_accession["html_link"] = None

# %% Retrieve links for all statements

links_full = {}
links_core = {}

df_statement_links = pd.DataFrame()
# df_report_links = pd.DataFrame()

for acc_num, row in df_accession.iterrows():
    acc_date = row["report_date"]

    # Get statement links
    links_statement_dict, links_statement_df = edgar_functions.get_statement_links(
        ticker,
        acc_num,
        acc_date,
        headers,
    )

    links_full[acc_num] = links_statement_dict[acc_num]
    links_statement_df.insert(loc=1, column="report_date", value=acc_date)

    df_statement_links = pd.concat([df_statement_links, links_statement_df], axis=0)

    # Get report links
    df_accession["ixbrl_link"].loc[acc_num] = edgar_functions.get_ixbrl_link(
        ticker, acc_num, headers
    )
    df_accession["html_link"].loc[acc_num] = edgar_functions.get_report_html(
        ticker, acc_num, headers
    )

    # Print progress
    print(f"{acc_num} ; {acc_date} ; links retrieved")

# %% Export dataframes to csv

print(df_accession)  # accession number and date relationship
print(df_statement_links)  # statement links and accession number relationship

df_statement_links.to_csv(os.path.join(path_ticker, f"{ticker}_statement_links.csv"))
df_accession.to_csv(os.path.join(path_ticker, f"{ticker}_accession_numbers.csv"))

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


# %% Export to database
# Drop tables if they exist with cascade
psql_conn.drop_table_if_exists(ticker_accession_numbers, engine, cascade=True)
psql_conn.drop_table_if_exists(ticker_statement_link, engine, cascade=True)

# % Exporting data
# Create sql tables from dataframes
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

# %%
