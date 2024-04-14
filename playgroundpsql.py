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


column_name = "accession_number"
table_name = "nvda_accession_numbers"
constraint_name = "nvda_accession_numbers_pkey"
engine = sqlalchemy.create_engine(
    f"{dialect}+psycopg://{username}:{password}@{host}:{port}/{dbname}",
    client_encoding="utf8",
)

# %%

# drop constraint
psql_conn.drop_constraint_if_exists(table_name, "nvda_accession_numbers_pkey", engine)

# add constraint
psql_conn.add_primary_key_if_not_exists(table_name, column_name, engine)


# %%

df = pd.read_sql_table(table_name, engine)


#%%
sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

x  = psql_conn.execute_query(sql, engine)

inspect_obj  = sqlalchemy.inspect(engine)