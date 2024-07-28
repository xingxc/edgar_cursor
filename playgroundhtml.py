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

import calendar

# %% Inputs and initilizing info
ticker = "nvda"
headers = {"User-agent": "email@email.com"}
path_statement_html = "/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker/nvda/statements_html"
path_statement_df = "/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/python/get_SEC_data/ticker/nvda/statements_df"


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
df_accession = pd.read_sql_table(ticker_accession_numbers, engine)

# Get the statement key mapping
df_statement_name_key_map = pd.read_sql_table("statement_key_mapping", engine)

# %% Get all the links and filtered links for the accession numbers

dict_statement_link_collector = {}
dict_statement_link_filtered = {}

# for i, row in df_accession.iterrows():
df_accession_row = df_accession.iloc[-1, :]
acc_num = df_accession_row["accession_number"]
report_date = df_accession_row["report_date"]
print(f"{acc_num} - {df_accession_row['report_date']} - {df_accession_row['form']}")

df_statement_links = psql_conn.get_sql_table_where_fk_equal(
    table_name_fk=ticker_statement_link,
    column_name_fk="accession_number",
    table_name_pk=ticker_accession_numbers,
    value_pk=acc_num,
    engine=engine,
)
dict_statement_link_collector[acc_num] = df_statement_links

dict_statement_link_filtered[acc_num] = edgar_functions.get_filtered_statement_links(
    df_statement_links, df_statement_name_key_map
)

# %% Collector and filtered links

display(dict_statement_link_collector[acc_num])
display(dict_statement_link_filtered[acc_num])


# %% Load accession into link collector

statement_name_series = dict_statement_link_collector[acc_num]["statement_name"]


# iterating through all the statements in the statement_name_series
# for k, ticker_statement_name in  enumerate(statement_name_series):
k = 8
k = 37

ticker_statement_name = statement_name_series[k]

statement_info = dict_statement_link_collector[acc_num][
    dict_statement_link_collector[acc_num]["statement_name"] == ticker_statement_name
]

statement_link = statement_info["statement_link"].iloc[0]

statement_soup = edgar_functions.get_statement_soup(statement_link, headers=headers)

name_html = f"{acc_num}_{ticker_statement_name}.html"

utility_belt.save_soup_to_html(
    statement_soup,
    os.path.join(path_statement_html, name_html),
)

print(f"html: {name_html}")

# % Find all statement soup with class as tag_filter (report)]
tag_filter = "report"
tag_list = statement_soup.find_all("table", class_=tag_filter)


# %% Iterate through all the tables in the statement_soup
tag_soup = tag_list[0]


# %% Get the dates

## Executed edgar_functions.get_datetime_index_dates_from_statement
# statement_date = edgar_functions.get_datetime_index_dates_from_statement(tag_soup)
table_dates = tag_soup.find_all("th", class_="th")
dates = [str(th.div.string) for th in table_dates if th.div and th.div.string]
dates = [edgar_functions.standardize_date(date).replace(".", "") for date in dates]

try:
    dates = pd.to_datetime(dates)
except Exception as e:
    print(f"Error converting dates to datetime: {e}")
## Executed edgar_functions.get_datetime_index_dates_from_statement

statement_date = dates.copy()

# %%

# Get the header
statement_header = tag_soup.find("th", class_="tl")
statement_header = statement_header.find_parent("tr")
statement_header = statement_header.text.strip().replace("\n", " ; ")

# %% 

classes_to_find = ["re", "ro", "rou", "reu"]

# rows = tag_soup.find_all(["tr", "th", "td"], class_=classes_to_find)
rows = tag_soup.find_all(["tr"], class_=classes_to_find)
statement_df = []
for row in rows:
    # cols = row.find_all(["tr", "th", "td"])  # This handles both th and td
    cols = row.find_all(["tr", "th", "td"])  # This handles both th and td
    cols = [ele.text.strip() for ele in cols]
    statement_df.append(cols)  # Add the data

statement_df = pd.DataFrame(statement_df)
statement_df = statement_df.set_index(0)
statement_df.index.name = statement_header
statement_df.columns = statement_date

statement_df.to_csv(
    os.path.join(path_statement_df, f"{acc_num}_{ticker_statement_name}_{i}.csv")
)

# %%

# Save the soup to html
for i, tag_soup in enumerate(tag_list):
    # utility_belt.save_soup_to_html(
    #     tag,
    #     os.path.join(
    #         path_statement_html,
    #         f"{acc_num}_{ticker_statement_name}_{tag_filter}_{i}.html",
    #     ),
    # )

    # % Start of iteration for all tables
    # i = 0
    # tag_soup = tag_list[i]

    # % Get the statement from tag_soup
    statement_date = edgar_functions.get_datetime_index_dates_from_statement(tag_soup)

    # %
    # table_headers = tag_soup.find_all("th", {"class": "th"})
    # table_headers = [str(th.div.string) for th in table_headers if th.div and th.div.string]
    # dates = [edgar_functions.standardize_date(date).replace(".", "") for date in table_headers]
    # pd.to_datetime(dates)

    # %
    # % Get the header from tag_soup

    # Get the header
    statement_header = tag_soup.find("th", {"class": "tl"})
    statement_header = statement_header.find_parent("tr")

    statement_header = statement_header.text.strip().replace("\n", " ; ")

    # print(statement_header)

    # % Get the data from tag_soup

    classes_to_find = ["re", "ro", "rou", "reu"]

    rows = tag_soup.find_all(["tr", "th", "td"], class_=classes_to_find)

    statement_df = []
    for row in rows:
        cols = row.find_all(["tr", "th", "td"])  # This handles both th and td
        cols = [ele.text.strip() for ele in cols]
        statement_df.append(cols)  # Add the data

    # % Construct DataFrame
    statement_df = pd.DataFrame(statement_df)
    statement_df = statement_df.set_index(0)
    statement_df.index.name = statement_header
    statement_df.columns = statement_date
    # statement_df.name = statement_name

    # display(statement_df)

    statement_df.to_csv(
        os.path.join(path_statement_df, f"{acc_num}_{ticker_statement_name}_{i}.csv")
    )

# %%
