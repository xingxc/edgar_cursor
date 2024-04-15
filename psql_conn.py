import sqlalchemy
import pandas as pd


def execute_query(query, engine):
    """
    Execute raw psql query using sqlalchemy

    Parameters:
        query: str
            psql query to be executed
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        result: sqlalchemy.engine.result.ResultProxy
            result of the query execution


    """
    try:
        with engine.connect() as connection:
            result = connection.execute(sqlalchemy.text(query))
            return result
    except Exception as e:
        print(f"An error occurred: {e}")


def drop_table_if_exists(table_name, engine, cascade=False):
    """
    Drop a table if it exists

    Parameters:
        table_name: str
            name of the table
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database
        cascade: bool
            drop table with cascade

    Returns:
        None
    """
    # Define the SQL command
    if cascade:
        sql = f"DROP TABLE IF EXISTS {table_name} CASCADE; COMMIT;"
    else:
        sql = f"DROP TABLE IF EXISTS {table_name}; COMMIT;"

    # Execute the SQL command
    execute_query(sql, engine)


def drop_constraint_if_exists(table_name, constraint_name, engine):
    """
    Drop a constraint from a table if the constraint exists

    Parameters:
        table_name: str
            name of the table
        constraint_name: str
            name of the constraint to be dropped
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        None
    """
    print(f"Before Drop Constraint: {get_constraints(table_name, engine)}")
    # Define the SQL command
    sql = (
        f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name}; COMMIT;"
    )
    # Execute the SQL command
    execute_query(sql, engine)
    print(f"After Drop Constraint: {get_constraints(table_name, engine)}")


def add_primary_key_if_not_exists(table_name, column_name, engine):
    """
    Add primary key to a table from an existing column if a primary key does not already exist

    Parameters:
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database
        table_name: str
            name of the table
        column_name: str
            name of the column to be used as primary key

    Returns:
        column_name: str
            name of the primary key column(s) of the table
    """

    print(f"Before Add Constraint: {get_constraints(table_name, engine)}")
    # Define the SQL command to check if a primary key already exists
    sql_check = f"SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary;"
    primary_key = execute_query(sql_check, engine)
    _ = primary_key.scalar()
    # If a primary key does not exist, add the primary key
    if not _:

        # Define the SQL command to add the primary key
        sql_add = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name}); COMMIT;"

        # Execute the SQL command
        execute_query(sql_add, engine)

        print(f"After Add Constraint: {get_constraints(table_name, engine)}")
        return column_name

    # If a primary key already exists, return its name
    else:
        print(f"Add Canceled: {get_constraints(table_name, engine)}")

        return _


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
    results = execute_query(sql_check, engine)
    if results.scalar():
        print(f"Foreign key {constraint_name_fk} already exists")
    else:
        # add foreign key
        print(f"Before Add Constraint: {get_constraints(table_name_fk, engine)}")
        sql_fk = f"ALTER TABLE {table_name_fk} ADD CONSTRAINT {constraint_name_fk} FOREIGN KEY ({column_name_fk}) REFERENCES {table_name_pk} ({column_name_pk}); COMMIT;"
        execute_query(sql_fk, engine)
        print(f"After Add Constraint: {get_constraints(table_name_fk, engine)}")


def get_primary_key(table_name, engine):
    """
    Get the primary key column(s) of a table

    Parameters:
        table_name: str
            name of the table
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        rows: list
            list of primary key column(s) of the table

    """

    sql = f"""
    SELECT kcu.column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY'
    """
    results = execute_query(sql, engine)
    rows = results.fetchall()
    return rows


def get_table_names_like(regex_in, engine):
    """
    Get all table names that match a regex pattern

    Parameters:
        regex_in: str
            regex pattern to match table names
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        df: pd.DataFrame
            dataframe of table names that match the regex pattern
    """

    sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name ~ '{regex_in}';"
    results = execute_query(sql, engine)
    df = pd.DataFrame(results.fetchall())
    return df


def get_column_names(table_name, engine):
    """
    Get the column names of a table

    Parameters:
        table_name: str
            name of the table
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        rows: list
            list of column names of the table

    """

    sql = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}'
    """
    results = execute_query(sql, engine)
    rows = results.fetchall()
    return rows


def get_constraints(table_name, engine):
    """

    Get the constraints of a table

    Parameters:
        table_name: str
            name of the table
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        rows: list
            list of constraints of the table

    """

    sql = f"""
    SELECT constraint_name
    FROM information_schema.table_constraints
    WHERE table_name = '{table_name}'
    """
    results = execute_query(sql, engine)
    rows = results.fetchall()
    return rows


def get_constraint_relationship(constraint_name, engine):
    """
    Get the relationship of a constraint

    Parameters:
        constraint_name: str
            name of the constraint
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        rows: list
            list of relationships of the constraint
    """

    sql = f"""
    SELECT 
        kcu.table_name, 
        kcu.column_name, 
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM 
        information_schema.key_column_usage AS kcu
    JOIN 
        information_schema.referential_constraints AS rc
    ON 
        kcu.constraint_name = rc.constraint_name
    JOIN 
        information_schema.constraint_column_usage AS ccu
    ON 
        rc.unique_constraint_name = ccu.constraint_name
    WHERE 
        kcu.constraint_name = '{constraint_name}';
    """

    results = execute_query(sql, engine)
    rows = results.fetchall()

    return rows


def get_sql_table_where_column_equal(
    table_name,
    column_name,
    value,
    engine,
):
    """
    Get all rows from a table where a column equals a specific value

    Parameters:
        table_name: str
            name of the table
        column_name: str
            name of the column
        value: str
            value of the column
        engine: sqlalchemy.engine.base.Connection
            sqlalchemy engine to connect to psql database

    Returns:
        df: pd.DataFrame
            dataframe of the rows where the column equals the value

    """

    sql = f"SELECT * FROM {table_name} WHERE {column_name} = '{value}';"

    results = execute_query(sql, engine)
    df = pd.DataFrame(results.fetchall(), columns=results.keys())
    return df


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

    result = execute_query(sql, engine)

    if result:
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
