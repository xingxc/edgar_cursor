import sqlalchemy


# create function to execute raw psql query using sqlalchemy, inputs of query and engine, returns result


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

    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(query))
        return result


def drop_constraint(table_name, constraint_name, engine):
    """
    Drop a constraint from a table

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

    # Define the SQL command
    sql = (
        f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name}; COMMIT;"
    )
    # Execute the SQL command
    execute_query(sql, engine)


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
        sql_add = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name}); COMMIT;"

        # Execute the SQL command
        execute_query(sql_add, engine)
        return column_name

    # If a primary key already exists, return its name
    else:
        return primary_key


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

    query = f"""
    SELECT kcu.column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY'
    """
    results = execute_query(query, engine)
    rows = results.fetchall()
    return rows


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

    query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}'
    """
    results = execute_query(query, engine)
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

    query = f"""
    SELECT constraint_name
    FROM information_schema.table_constraints
    WHERE table_name = '{table_name}'
    """
    results = execute_query(query, engine)
    rows = results.fetchall()
    return rows


def get_constraint_relationship(constraint_name, engine):
    query = f"""
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

    results = execute_query(query, engine)

    rows = results.fetchall()

    return rows


# not working


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
    results = execute_query(query, engine)
    return results
