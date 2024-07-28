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
ticker = "nvda"
headers = {"User-agent": "email@email.com"}
path_ticker = "/Users/johnxing/Documents/Documents - Apple Mac Mini/finances/stocks/ticker/nvda/python"
path_statement_html = f"{path_ticker}/statements_html"
path_statement_df = f"{path_ticker}/statements_df"


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
form_name = df_accession_row["form"]
print(f"{acc_num} - {report_date} - {form_name}")

# return dataframe from "accession number table" and "statement link table" for specific accession number
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

# %%

statement_name_series = dict_statement_link_collector[acc_num]["statement_name"]

statement_log = pd.DataFrame(
    columns=[
        "report_date",
        "form",
        "accession_number",
        "statement_name",
        "statement_link",
        "tables_scaped",
    ]
)

for ticker_statement_name in statement_name_series:

    # ticker_statement_name = statement_name_series[0]

    statement_info = dict_statement_link_collector[acc_num][
        dict_statement_link_collector[acc_num]["statement_name"]
        == ticker_statement_name
    ]
    statement_info["tables_scaped"] = 0

    # Get statement soup from link
    statement_link = statement_info["statement_link"].iloc[0]
    statement_soup = edgar_functions.get_statement_soup(statement_link, headers=headers)

    # Save the soup to html
    utility_belt.save_soup_to_html(
        statement_soup,
        os.path.join(
            path_statement_html, f"{report_date}_{acc_num}_{ticker_statement_name}.html"
        ),
    )

    print(f"html: {acc_num} ; {ticker_statement_name}")

    # % Find all statement soup with class as tag_filter
    tag_filter = "report"
    tag_list = statement_soup.find_all("table", class_=tag_filter)

    # % Start of iteration for all tables
    for i, tag_soup in enumerate(tag_list):
        try:
            # % Get the date
            statement_date = edgar_functions.get_datetime_index_dates_from_statement(
                tag_soup
            )

            # Get the header
            statement_header = tag_soup.find("th", class_="tl")
            statement_header = statement_header.find_parent("tr")
            statement_header = statement_header.text.strip().replace("\n", " ; ")
            print(statement_header)

            # % Get the data from tag_soup
            classes_to_find = ["re", "ro", "rou", "reu"]
            tag_soup_rows = tag_soup.find_all(
                ["tr", "th", "td"], class_=classes_to_find
            )

            statement_df = []
            for row in tag_soup_rows:
                cols = row.find_all(["tr", "th", "td"])  # This handles both th and td
                cols = [ele.text.strip() for ele in cols]
                statement_df.append(cols)  # Add the data

            # % Construct DataFrame
            statement_df = pd.DataFrame(statement_df)
            statement_df = statement_df.set_index(0)
            statement_df.index.name = statement_header
            statement_df.columns = statement_date
            # statement_df.name = statement_name

            display(statement_df)

            statement_df.to_csv(
                os.path.join(
                    path_statement_df,
                    f"{report_date}_{acc_num}_{ticker_statement_name}_{i}.csv",
                )
            )
            statement_info["tables_scaped"] += 1
        except Exception as e:
            print(f"Error: {e}")

    statement_log = pd.concat([statement_log, statement_info], ignore_index=True)

# Output the statement log for the statements that are outputed
print(f"{acc_num} - {report_date} - {form_name}")
statement_log.to_csv(
    f"{path_ticker}/{report_date}_{acc_num}_{form_name}_edgar_statement_log.csv"
)


# %%


# %%

# Save the soup to html
# for i, tag in enumerate(tag_list):
#     # utility_belt.save_soup_to_html(
#     #     tag,
#     #     os.path.join(
#     #         path_statement_html,
#     #         f"{acc_num}_{ticker_statement_name}_{tag_filter}_{i}.html",
#     #     ),
#     # )

#     # % Start of iteration for all tables
#     # i = 0
#     tag_soup = tag_list[i]

#     # % Get the statement from tag_soup
#     statement_date = edgar_functions.get_datetime_index_dates_from_statement(
#         tag_soup
#     )

#     # %
#     # table_headers = tag_soup.find_all("th", {"class": "th"})
#     # table_headers = [str(th.div.string) for th in table_headers if th.div and th.div.string]
#     # dates = [edgar_functions.standardize_date(date).replace(".", "") for date in table_headers]
#     # pd.to_datetime(dates)

#     # %
#     # % Get the header from tag_soup

#     # Get the header
#     statement_header = tag_soup.find("th", {"class": "tl"})
#     statement_header = statement_header.find_parent("tr")

#     statement_header = statement_header.text.strip().replace("\n", " ; ")

#     print(statement_header)

#     # % Get the data from tag_soup

#     classes_to_find = ["re", "ro", "rou", "reu"]

#     rows = tag_soup.find_all(["tr", "th", "td"], class_=classes_to_find)

#     statement_df = []
#     for row in rows:
#         cols = row.find_all(["tr", "th", "td"])  # This handles both th and td
#         cols = [ele.text.strip() for ele in cols]
#         statement_df.append(cols)  # Add the data

#     # % Construct DataFrame
#     statement_df = pd.DataFrame(statement_df)
#     statement_df = statement_df.set_index(0)
#     statement_df.index.name = statement_header
#     statement_df.columns = statement_date
#     # statement_df.name = statement_name

#     display(statement_df)


# %% Original get df
# (
#     columns,
#     _,
#     values_set,
#     index_dates,
# ) = edgar_functions.extract_columns_values_and_dates_from_statement(statement_soup)
# df_income_statement = edgar_functions.get_statement_df(statement_link, headers)


# %%
cols = [ele.text.strip() for ele in cols]
data.append(cols)  # Add the data
print(cols.__len__())

# %%

# Iterate through rows to extract data
for row in rows:
    cols = row.find_all(["tr", "th", "td"])  # This handles both th and td
    cols = [ele.text.strip() for ele in cols]
    data.append(cols)  # Add the data
    print(cols.__len__())


# Create DataFrame
df = pd.DataFrame(data)

# print(df)
# %%

# Assuming 'data' is your list of uneven lists
data = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]

# Create a DataFrame
df = pd.DataFrame(data)

print(df)


# %%

# %% Export the income statement to the database
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
    table_name_fk=ticker_statement_link,
    column_name_fk="accession_number",
    table_name_pk=ticker_accession_numbers,
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
    AND kcu.table_name = '{ticker_accession_numbers}' 
    AND tco.table_name = '{ticker_statement_link}'
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
