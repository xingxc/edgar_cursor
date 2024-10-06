# %% Imports
import re
import os
import requests
import subprocess
import utility_belt
import pandas as pd
import edgar_functions
import psql_conn
import sqlalchemy

# %% Inputs and initilizing info

headers = {"User-agent": "email@email.com"}
path_tickers = "/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker"
ticker = "nvda"

# %% Create ticker folder and subfolders
path_ticker = os.path.join(path_tickers, ticker.lower())
path_statement_html = os.path.join(path_ticker, "statements_html")
path_statement_df = os.path.join(path_ticker, "statements_df")

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
df_statement_name_key_map = pd.read_sql_table("statement_key_mapping", engine)

# %% Get dictionary for all statement types

paths_statement_df = utility_belt.find_files_with_regex(path_statement_df, ".*.csv")
keys_statement_df = list(paths_statement_df.keys())
keys_statement_df.sort()
key = keys_statement_df[-1]

statement_path_df = pd.DataFrame(
    columns=["report_date", "acc_num", "acc_form", "statement_type", "path"]
)
statement_path_df.index.name = ""

for i, key in enumerate(keys_statement_df):
    # print(i, key)
    _, acc_date, acc_form, acc_num, statement_type, _, _ = key.split("_")
    paths_statement_df[key]

    statement_path_df.loc[i] = [
        acc_date,
        acc_num,
        acc_form,
        statement_type,
        paths_statement_df[key],
    ]

statement_path_keys = list(df_statement_name_key_map.keys())
statement_path_dict = {}
for statement_type in statement_path_keys:
    statement_path_dict[statement_type] = statement_path_df[
        statement_path_df["statement_type"] == statement_type
    ]


# %% Construct dataframe for each statement type and export to csv

statement_construct_dict = {}

key_statement = statement_path_keys[3]

for key_statement in statement_path_keys:
    df_construct = pd.DataFrame()
    for i, row in statement_path_dict[key_statement].iterrows():
        df = pd.read_csv(row["path"])

        df_construct = pd.concat([df_construct, df], axis=1)
    statement_construct_dict[key_statement] = df_construct
    df_construct.to_csv(f"{path_ticker}/{ticker}_CONSTRUCTED_{key_statement}.csv")
    print(f"OUTPUT: {path_ticker}/{ticker}_CONSTRUCTED_{key_statement}.csv")
