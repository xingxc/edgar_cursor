# %% Imports
import psql_conn
import os
import sqlalchemy
import pandas as pd
import psycopg
import requests
import numpy as np
import edgar_functions
from bs4 import BeautifulSoup


# %% Inputs and initilizing info

headers = {"User-agent": "email@email.com"}
ticker = "team"


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

# %%


psql_conn.get_all_constraints(engine)


# %%
# psql_conn.get_table_names_like(".*", engine)
# psql_conn.get_table_names_like("nvda", engine)
# psql_conn.get_constraints("nvda_accession_numbers", engine)

df = psql_conn.get_all_constraints(engine)


# %% Get 10k and 10q accession numbers:

acc_10k = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=True, accession_number_only=False
)
acc_10q = edgar_functions.get_filter_filing(
    ticker, headers=headers, ten_k=False, accession_number_only=False
)

df_accession = pd.concat([acc_10k, acc_10q], axis=0)
df_accession.sort_index(inplace=True)


# %%
