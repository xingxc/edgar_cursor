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


# drop constraint
psql_conn.drop_constraint_if_exists(table_name, "nvda_accession_numbers_pkey", engine)

# add primary key
psql_conn.add_primary_key_if_not_exists(table_name, column_name, engine)

# %%

table_name_fk = "nvda_statement_links"
column_name_fk = "accession_number"
constraint_name_fk = "fk_accession_number"

table_name_pk = "nvda_accession_numbers"
column_name_pk = "accession_number"
value_pk = "000104581024000029"

# add foreign key
psql_conn.add_foreign_key_if_not_exists(
    table_name_fk,
    column_name_fk,
    constraint_name_fk,
    table_name_pk,
    column_name_pk,
    engine,
)

# psql_conn.get_constraints(table_name_fk, engine)


df_column = psql_conn.get_sql_table_where_column_equal(
    table_name_fk,
    column_name_fk,
    value_pk,
    engine,
)


df_fk = psql_conn.get_sql_table_where_fk_equal(
    table_name_fk,
    column_name_fk,
    table_name_pk,
    value_pk,
    engine,
)

display(df_column)
display(df_fk)

# %%


def get_sql_table_where_fk_equal(
    table_name_fk,
    column_name_fk,
    table_name_pk,
    value_pk,
    engine,
):
    """
    Get the result of a query where the foreign key equals a value

    Parameters:
        table_name_fk: str
            name of the table with the foreign key
        column_name_fk: str
            name of the column with the foreign key
        table_name_pk: str
            name of the table with the primary key
        value_pk: str
            value of the primary key
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        df: pandas.DataFrame
            dataframe of the result of the query
    """

    sql = f"""
            SELECT a.report_date, a.form, t.accession_number, t.statement_link FROM {table_name_fk} t
            JOIN {table_name_pk} a ON t.{column_name_fk} = a.{column_name_fk}
            WHERE t.{column_name_fk} = '{value_pk}';
            """

    result = psql_conn.execute_query(sql, engine)

    if result:
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df



# %%


sql = f"SELECT * FROM {table_name_fk} t JOIN {table_name_pk} a ON t.{column_name_fk} = a.{column_name_fk} WHERE t.{column_name_fk} = '{value_pk}';"


# def get_sql_table_where_fk_equal(
#     table_name_fk,
#     column_name_fk,
#     table_name_pk,
#     value_pk,
#     engine,
# ):
#     """
#     Get the result of a query where the foreign key equals a value

#     Parameters:
#         table_name_fk: str
#             name of the table with the foreign key
#         column_name_fk: str
#             name of the column with the foreign key
#         table_name_pk: str
#             name of the table with the primary key
#         value_pk: str
#             value of the primary key
#         engine: sqlalchemy.engine.base.Connection
#             sqlalchemy engine to connect to psql database

#     Returns:
#         df: pandas.DataFrame
#             dataframe of the result of the query
#     """

#     sql = f"SELECT * FROM {table_name_fk} t JOIN {table_name_pk} a ON t.{column_name_fk} = a.{column_name_fk} WHERE t.{column_name_fk} = '{value_pk}';"

#     result = psql_conn.execute_query(sql, engine)

#     df = pd.DataFrame(result.fetchall(), columns=result.keys())

#     return df


# %% read sql tables

table_name = "nvda_statement_links"
column_name = "accession_number"
value = "000104581014000188"

# read sql table where column equals value
psql_conn.get_sql_table_where_column_equal(table_name, column_name, value, engine)

# read entire sql table to dataframe
df = pd.read_sql_table(table_name, engine)

# read all table names with regex pattern
regex_in = "nvda.*"
psql_conn.get_table_names_like(regex_in, engine)

# %%


# %%
sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
x = psql_conn.execute_query(sql, engine)
inspect_obj = sqlalchemy.inspect(engine)


# %%


value = "000104581014000188"

# %%


# %%


# %%

# create function to add foreign key to existing table if it does not exist


def add_foreign_key_if_not_exists(
    table_name_fk,
    column_name_fk,
    constraint_name_fk,
    table_name_pk,
    column_name_pk,
    engine,
):
    """
    Add a foreign key to a table if it does not exist

    Parameters:
        table_name_fk: str
            name of the table with the foreign key
        column_name_fk: str
            name of the column with the foreign key
        constraint_name_fk: str
            name of the foreign key constraint
        table_name_pk: str
            name of the table with the primary key
        column_name_pk: str
            name of the primary key column
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        None
    """
    sql_check = f"SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_name = '{constraint_name_fk}' AND constraint_type = 'FOREIGN KEY';"
    # check if foreign key exists
    results = psql_conn.execute_query(sql_check, engine)
    if results.scalar():
        print(f"Foreign key {constraint_name_fk} already exists")
    else:
        # add foreign key
        print(
            f"Before Add Constraint: {psql_conn.get_constraints(table_name_fk, engine)}"
        )
        sql_fk = f"ALTER TABLE {table_name_fk} ADD CONSTRAINT {constraint_name_fk} FOREIGN KEY ({column_name_fk}) REFERENCES {table_name_pk} ({column_name_pk}); COMMIT;"
        psql_conn.execute_query(sql_fk, engine)
        print(
            f"After Add Constraint: {psql_conn.get_constraints(table_name_fk, engine)}"
        )
