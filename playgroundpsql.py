# %%
import psql_conn
import os
import sqlalchemy
import pandas as pd


dialect = "postgresql"
username = os.getenv("DATABASE_USER")
password = os.getenv("DATABASE_PASSWORD")
host = "localhost"
port = "5432"
db_name = "test"

engine = sqlalchemy.create_engine(
    f"{dialect}+psycopg://{username}:{password}@{host}:{port}/{db_name}"
)


column_name = "accession_number"
table_name = "nvda_accession_numbers"
constraint_name = "2200_24679_1_not_null"

# psql_conn.get_constraints("nvda_accession_numbers", engine)

# psql_conn.get_constraint_relationship("2200_24679_1_not_null", engine)

df = pd.read_sql_table(table_name, engine, index_col="accession_number")

# %%

# Create the SQL statement
sql = sqlalchemy.text(
    f"""
ALTER TABLE {table_name}
ADD PRIMARY KEY ({column_name});
"""
)

# Execute the SQL statement
with engine.connect() as connection:
    connection.execute(sql)


# %%


# %%


