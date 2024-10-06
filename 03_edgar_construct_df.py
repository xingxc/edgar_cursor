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
ticker = "team"

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

# %% Get accession numbers and statement key mapping

# table names and primary/foreign key constraints
ticker_accession_numbers = f"{ticker}_accession_numbers"
ticker_statement_link = f"{ticker}_statement_link"

# alternative methods
# ticker_accession_numbers = psql_conn.get_table_names_like(
#     f"{ticker}_accession_numbers", engine
# )["table_name"].iloc[0]

# ticker_statement_link = psql_conn.get_table_names_like(f"{ticker}_statement_link", engine)[
#     "table_name"
# ].iloc[0]

# return the primary key column name from tables
accession_pk_column = psql_conn.get_primary_key(ticker_accession_numbers, engine)
links_pk_column = psql_conn.get_primary_key(ticker_statement_link, engine)

links_fk_column = "accession_number"
links_fk_constraint_name = "fk_accession_number"

# Get the table of accession numbers for the ticker
df_accession = pd.read_sql_table(
    ticker_accession_numbers, engine, index_col="accession_number"
)

display(df_accession)

# Get the statement key mapping
# df_statement_name_key_map = pd.read_sql_table("statement_key_mapping", engine)


# %%
paths_df = {
    i: os.path.join(path_statement_df, i) for i in os.listdir(path_statement_df)
}
keys_df = list(paths_df.keys())

keys_balance_sheet = 

acc_num = df_accession.index[-1]




# %%

key = keys_df[0]


df = pd.read_csv(paths_df[key])

# pd.read_csv
