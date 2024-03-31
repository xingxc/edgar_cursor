# %%

########## ------ START OF SCRIPT ------ ##########

# Imports
import os
import psycopg
import requests
import subprocess
import utility_belt
import pandas as pd
import edgar_functions
from sqlalchemy import create_engine


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

df_accession = pd.concat([acc_10k, acc_10q], axis=0)
df_accession.sort_index(inplace=True)


# %% Retrieve links for all statements

links_full = {}
links_core = {}

df_statement_links = pd.DataFrame()

for acc_date, row in df_accession.iterrows():
    acc_num = row["accessionNumber"]

    links_statement_dict, links_statement_df = edgar_functions.get_statement_links(
        ticker,
        acc_num,
        acc_date,
        headers,
    )

    links_full[acc_num] = links_statement_dict[acc_num]
    df_statement_links = pd.concat([df_statement_links, links_statement_df], axis=0)
    print(f"{acc_num} ; {acc_date} ; links retrieved")

df_statement_links.reset_index(drop=True, inplace=True)

# % Filter for core links
for acc_num, links in links_full.items():
    print(f"{acc_num}")
    links_core[acc_num] = edgar_functions.filter_links(links, path_dict["json"])

# %% Create postgres engine and export accession numbers to postgres database

dialect = "postgresql"
username = os.getenv("DATABASE_USER")
password = os.getenv("DATABASE_PASSWORD")
host = "localhost"
port = "5432"
db_name = "test"

engine = create_engine(
    f"{dialect}+psycopg://{username}:{password}@{host}:{port}/{db_name}"
)

# Export links to postgres database
table_name_accession = f"{ticker}_accession_numbers"
table_name_links = f"{ticker}_statement_links"

df_accession.to_sql(table_name_accession, engine, if_exists="replace", index=True)
df_statement_links.to_sql(table_name_links, engine, if_exists="replace", index=False)


# %% Export accession numbers and links to ticker folder

utility_belt.export_json_file(
    os.path.join(path_dict["ticker"], f"{ticker}_links_full.json"), links_full
)
utility_belt.export_json_file(
    os.path.join(path_dict["ticker"], f"{ticker}_links_core.json"), links_core
)
df_accession.to_csv(os.path.join(path_dict["ticker"], "accession_numbers.csv"))

########## ------ END OF SCRIPT ------ ##########
