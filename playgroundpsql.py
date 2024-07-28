# %%
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


#%%


# %% add and drop constraints

# drop table
# psql_conn.drop_table_if_exists("nvda_accession_numbers", engine, cascade=True)
# psql_conn.drop_table_if_exists("nvda_statement_link", engine, cascade=True)
# psql_conn.drop_table_if_exists("statement_key_mapping", engine, cascade=True)


# list constraints

psql_conn.get_constraints("nvda_accession_numbers", engine)
psql_conn.get_constraints("nvda_statement_link", engine)
psql_conn.get_constraints("statement_key_mapping", engine)


# drop constraint
# psql_conn.drop_constraint_if_exists(table_name, "nvda_accession_numbers_pkey", engine)
# psql_conn.drop_constraint_if_exists(table_name, "nvda_accession_numbers_pkey", engine)
# psql_conn.drop_constraint_if_exists(table_name, "nvda_accession_numbers_pkey", engine)

# add primary key
# psql_conn.add_primary_key_if_not_exists(table_name, column_name, engine)


# %%

