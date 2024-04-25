# %%
import psql_conn
import os
import sqlalchemy
import pandas as pd
import psycopg


dialect = "postgresql"
username = os.getenv("DATABASE_USER")
password = os.getenv("DATABASE_PASSWORD")
host = "localhost"
port = "5432"
dbname = "test"

ticker = "nvda"
column_name = "accession_number"
table_name = "nvda_accession_numbers"
constraint_name = "nvda_accession_numbers_pkey"
engine = sqlalchemy.create_engine(
    f"{dialect}+psycopg://{username}:{password}@{host}:{port}/{dbname}",
    client_encoding="utf8",
)

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


def get_statement_name_key_map(df_statement_links, statement_name_key_map):
    _ = {}
    for i, possible_key in df_statement_links["statement_name"].items():
        for statement_name, possible_keys_series in statement_name_key_map.items():
            index_series = possible_keys_series[
                possible_keys_series == possible_key
            ].index
            if not index_series.empty:
                _[statement_name] = i
                break

    _ = list(_.values())

    df_statement_filtered = df_statement_links.loc[_]

    return df_statement_filtered
