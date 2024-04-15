# %%
# Imports
import os
import requests
import subprocess
import utility_belt
import pandas as pd
import edgar_functions
import psql_conn
import sqlalchemy

ticker = "nvda"
headers = {"User-agent": "email@email.com"}


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

# Get the table names
table_name_accession = psql_conn.get_table_names_like(
    f"{ticker}_accession_numbers.*", engine
)["table_name"].iloc[0]
table_name_links = psql_conn.get_table_names_like(
    f"{ticker}_statement_links.*", engine
)["table_name"].iloc[0]

# Get the tables as dataframes from table names
df_accession = pd.read_sql_table(table_name_accession, engine)


# %%

acc_num = df_accession["accession_number"].iloc[-1]

df_statement_links = psql_conn.get_sql_table_where_fk_equal(
    table_name_fk=table_name_links,
    column_name_fk="accession_number",
    table_name_pk=table_name_accession,
    value_pk=acc_num,
    engine=engine,
)


# %%
report_dates = df_accession["report_date"]

for i, acc_date in report_dates.items():
    acc_num = df_accession.loc[i, "accession_number"]

    df_statement_links[df_statement_links["accession_number"] == acc_num]


# %%
class ticker_statement:
    def __init__(self, ticker, headers):
        self.ticker = ticker
        self.headers = headers

        self.cik = edgar_functions.cik_matching_ticker(ticker, headers=headers)


# %% Inputs and initilizing info

headers = {"User-agent": "email@email.com"}
ticker = "nvda"

path_dict = {
    "ticker": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker",
    "json": r"/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/statement_key_mapping.json",
}
path_dict["ticker"] = os.path.join(path_dict["ticker"], ticker.lower())
path_dict["statements"] = os.path.join(path_dict["ticker"], "statements")

utility_belt.mkdir(path_dict["statements"])

# Get link paths and accession dataframe
command = (
    f"find '{path_dict['ticker']}' -type f \( -regex '.*.json' -o -regex '.*.csv' \) "
)
std_out = subprocess.run(command, shell=True, text=True, capture_output=True)
std_out = std_out.stdout.splitlines()

for link in std_out:

    # Get the base name of the link
    base_name = subprocess.run(
        f"basename '{link}' | rev | cut -d. -f2- | rev",
        shell=True,
        text=True,
        capture_output=True,
    ).stdout.strip()

    # Add the link to the path_dict
    path_dict[base_name] = link

# %% Import links and accession numbers

links_core = utility_belt.import_json_file(path_dict["nvda_links_core"])
df_acc = pd.read_csv(path_dict["accession_numbers"], index_col=0)
report_dates = list(df_acc.index)

# %% Get statements

statements_dict_df = {}
acc_date = report_dates[-1]

for acc_date in report_dates[-13:]:
    form_name = df_acc.loc[acc_date]["form"].replace("-", "")

    for statement_name, statement_link in links_core[acc_date].items():
        statements_dict_df[statement_name] = edgar_functions.get_statement_df(
            statement_link, headers
        )
        path_statement_out = os.path.join(
            path_dict["statements"],
            f"{statement_name}_{acc_date}_{form_name}.csv",
        )

        statements_dict_df[statement_name].to_csv(path_statement_out)

        print(f"export: {acc_date} ; {form_name} ; {statement_name}")
# %%
