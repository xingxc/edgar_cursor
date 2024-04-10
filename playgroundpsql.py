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
engine = sqlalchemy.create_engine(
    f"{dialect}+psycopg://{username}:{password}@{host}:{port}/{dbname}",
    client_encoding="utf8",
)

constraints_accession = psql_conn.get_constraints("nvda_accession_numbers", engine)
print(constraints_accession)
psql_conn.drop_constraint(table_name, "nvda_accession_numbers_pkey", engine)

constraints_accession = psql_conn.get_constraints("nvda_accession_numbers", engine)
print(constraints_accession)

constraints_accession = psql_conn.get_constraints("nvda_accession_numbers", engine)
print(constraints_accession)

# constraints_statement = psql_conn.get_constraints("nvda_statement_links", engine)
# print(constraints_statement)
# print(constraints_accession)
# print(constraints_statement)


# column_name_accession = psql_conn.get_column_names("nvda_accession_numbers", engine)
# column_name_statement = psql_conn.get_column_names("nvda_statement_links", engine)

# print(column_name_accession)
# print(column_name_statement)


# %%
table_name = "nvda_accession_numbers"
primary_key_column = column_name_accession[0][0]
query = f"ALTER TABLE {table_name} ADD PRIMARY KEY({primary_key_column});"
print(query)
# query = sqlalchemy.text(query).execution_options(autocommit=True)

# with engine.connect() as connection:
#     result = connection.execute(query)


# %%

engine = sqlalchemy.create_engine(
    f"{dialect}+psycopg://{username}:{password}@{host}:{port}/{dbname}",
    client_encoding="utf8",
)


def drop_constraint(table_name, constraint_name, engine):

    # Define the SQL command
    sql = sqlalchemy.text(
        f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name}; COMMIT;"
    )
    print(sql)
    # Execute the SQL command
    with engine.connect() as connection:
        connection.execute(sql)


def add_primary_key(table_name, column_name, engine):
    """
    Add primary key to a table from an existing column

    Parameters:
        table_name: str
            name of the table
        column_name: str
            name of the column to be used as primary key
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        results: list
            list of primary key column(s) of the table
    """

    query = f"""
    ALTER TABLE {table_name}
    ADD PRIMARY KEY ({column_name}); COMMIT;
    """
    results = psql_conn.execute_query(query, engine)
    return results


def add_primary_key_if_not_exists(engine, table_name, column_name):
    # Define the SQL command to check if a primary key already exists
    sql_check = sqlalchemy.text(
        f"""
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{table_name}'::regclass
        AND    i.indisprimary;
    """
    )

    # Execute the SQL command
    with engine.connect() as connection:
        result = connection.execute(sql_check)
        primary_key = result.scalar()

    # If a primary key does not exist, add the primary key
    if not primary_key:
        # Define the SQL command to add the primary key
        sql_add = sqlalchemy.text(
            f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name}); COMMIT;"
        )

        # Execute the SQL command
        with engine.connect() as connection:
            connection.execute(sql_add)

        return column_name

    # If a primary key already exists, return its name
    else:
        return primary_key


# constraints_accession = psql_conn.get_constraints(table_name, engine)

# %%

print("------------ Drop primary key before------------ ")
constraints_accession = psql_conn.get_constraints("nvda_accession_numbers", engine)
print(constraints_accession)

print("------------ Drop primary key after------------ ")
drop_constraint(table_name, "nvda_accession_numbers_pkey", engine)
constraints_accession = psql_conn.get_constraints("nvda_accession_numbers", engine)
print(constraints_accession)


# %%

print("------------ Drop primary key before------------ ")
constraints_accession = psql_conn.get_constraints("nvda_accession_numbers", engine)
print(constraints_accession)

print("------------ Drop primary key after------------ ")
add_primary_key(table_name, primary_key_column, engine)
constraints_accession = psql_conn.get_constraints("nvda_accession_numbers", engine)
print(constraints_accession)


# constraints_accession = psql_conn.get_constraints(table_name, engine)
# print(constraints_accession)

# %%

# dialect = "postgresql"
# username = os.getenv("DATABASE_USER")
# password = os.getenv("DATABASE_PASSWORD")
# host = "localhost"
# port = "5432"
# dbname = "test"

# # Define the SQL command
# table_name = "nvda_accession_numbers"
# primary_key_column = "accession_number"  # replace with your column name
# sql = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key_column});"


# # Establish a connection to the PostgreSQL database
# connection = psycopg.connect(
#     dbname=dbname,
#     user=username,
#     password=password,
#     host=host,
#     port="port",
# )


# def execute_query(sql, connection):
#     """
#     Execute raw psql query using psycopg

#     Parameters:
#         sql: str
#             psql query to be executed
#         connection: psycopg.connection
#             psycopg connection to psql database

#     Returns:
#         None
#     """

#     try:
#         # Create a cursor object
#         cursor = connection.cursor()

#         # Execute the SQL command
#         cursor.execute(sql)

#         # Commit the changes
#         connection.commit()

#     except (Exception, psycopg.DatabaseError) as error:
#         print(f"Error: {error}")
#     finally:
#         # Close the cursor and the connection
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()
