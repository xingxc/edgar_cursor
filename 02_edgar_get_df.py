# %% Imports
import os
import requests
import subprocess
import utility_belt
import pandas as pd
import edgar_functions
import psql_conn
import sqlalchemy


# %% Inputs and initilizing info
ticker = "nvda"
headers = {"User-agent": "email@email.com"}


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
accession_table_name = f"{ticker}_accession_numbers"
links_table_name = f"{ticker}_statement_link"

accession_pk_column = psql_conn.get_primary_key(accession_table_name, engine)
links_pk_column = psql_conn.get_primary_key(links_table_name, engine)

links_fk_column = "accession_number"
links_fk_constraint_name = "fk_accession_number"

# Get the table names
table_name_accession = psql_conn.get_table_names_like(
    f"{ticker}_accession_numbers", engine
)["table_name"].iloc[0]

table_name_links = psql_conn.get_table_names_like(f"{ticker}_statement_link", engine)[
    "table_name"
].iloc[0]

# Get the table of accession numbers for the ticker
df_accession = pd.read_sql_table(table_name_accession, engine)

# Get the statement key mapping
df_statement_name_key_map = pd.read_sql_table("statement_key_mapping", engine)


# %% Get all the links and filtered links for the accession numbers

dict_statement_link_collector = {}
dict_statement_link_filtered = {}

# for i, row in df_accession.iterrows():
row = df_accession.iloc[-1, :]
acc_num = row["accession_number"]
report_date = row["report_date"]
print(f"{acc_num} - {row['report_date']} - {row['form']}")

df_statement_links = psql_conn.get_sql_table_where_fk_equal(
    table_name_fk=table_name_links,
    column_name_fk="accession_number",
    table_name_pk=table_name_accession,
    value_pk=acc_num,
    engine=engine,
)
dict_statement_link_collector[acc_num] = df_statement_links

dict_statement_link_filtered[acc_num] = edgar_functions.get_filtered_statement_links(
    df_statement_links, df_statement_name_key_map
)


# %% Get the income statement for the accession number

income_statement_table_name = f"{ticker}_{acc_num}_{report_date}_income_statement"
statement_name = "income_statement"


# %%
df = dict_statement_link_filtered[acc_num]
statement_link = df["statement_link"][df["statement_name"] == statement_name][0]

df_income_statement = edgar_functions.get_statement_df(statement_link, headers)

print(f"export to database: {income_statement_table_name}")
df_income_statement.to_sql(
    income_statement_table_name,
    engine,
    if_exists="replace",
    index=True,
)


# %%


# def get_statement_name_key_map(df_statement_links, statement_name_key_map):
#     _ = {}
#     for i, possible_key in df_statement_links["statement_name"].items():
#         for statement_name, possible_keys_series in statement_name_key_map.items():
#             index_series = possible_keys_series[possible_keys_series == possible_key].index
#             if not index_series.empty:
#                 _[statement_name] = i
#                 break


#     _ = list(_.values())

#     dict_statement_link_collector[acc_num].loc[_]

#     return dict_statement_link_collector


# possible_keys_series = statement_name_key_map[statement]
# print(f"{statement} - {possible_keys_series}")


# for i, (statement_name, possible_keys_series) in enumerate(
#     statement_name_key_map.items()
# ):

# for possible_key in possible_keys_series:
#     index = possible_keys_series[possible_keys_series == possible_key].index

# statement_link = links_dict.get(possible_key, None)

# if statement_link is not None:

#     acc_num = links_dict["accession_date"]
#     links_filtered[statement_name] = statement_link

#     print(f"Retrieved: {statement_name} - {statement_link}")
#     statement_df.loc[i] = pd.Series(
#         {
#             "accession_date": acc_num,
#             "statement_name": statement_name,
#             "statement_link": statement_link,
#         },
#     )


# %%

# Get the table of statement links for the accession number
acc_num = df_accession["accession_number"].iloc[-1]

df_statement_links["statement_name"]


df_statement_links = psql_conn.get_sql_table_where_fk_equal(
    table_name_fk=table_name_links,
    column_name_fk="accession_number",
    table_name_pk=table_name_accession,
    value_pk=acc_num,
    engine=engine,
)
display(df_statement_links)

# %%

# %%

statement_link = df_statement_links["statement_link"].iloc[0]

edgar_functions.get_statement_df(statement_link, headers)


# %%

table_name_pk = "nvda_accession_numbers"
table_name_fk = "nvda_statement_link"

sql = f"""
SELECT kcu.column_name 
FROM information_schema.table_constraints tco 
JOIN information_schema.key_column_usage kcu 
    ON kcu.constraint_name = tco.constraint_name 
    AND kcu.constraint_schema = tco.constraint_schema 
WHERE tco.constraint_type = 'FOREIGN KEY' 
    AND kcu.table_name = '{accession_table_name}' 
    AND tco.table_name = '{links_table_name}'
"""

# Execute the SQL command
result = psql_conn.execute_query(sql, engine)

# Get the first row of the result set
row = result.fetchall()
display(row)

# %%

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
