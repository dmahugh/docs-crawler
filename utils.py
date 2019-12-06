"""Utility functions for Cloud SQL and related services.
"""
from contextlib import suppress
import csv
import shutil

import pyodbc


def csv_insert(*, cursor=None, database=None, table=None, filename=None):
    """Insert data from a CSV file into a table.

    Args:
        cursor: pyodbc cursor, connected to SQL Server database instance
        database: database name
        table: table name (table must already exist)
        filename: name of a CSV file to be imported. Must include a header row,
                  and each column name in the header must exactly match a
                  column name in the table.
    """
    if not cursor:
        raise ValueError("csv_insert: missing required argument (cursor)")
    if not database:
        raise ValueError("csv_insert: missing required argument (database)")
    if not database:
        raise ValueError("csv_insert: missing required argument (database)")
    if not table:
        raise ValueError("csv_insert: missing required argument (table)")
    if not filename:
        raise ValueError("csv_insert: missing required argument (filename)")

    cursor.execute(f"USE {database}")
    with open(filename, "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        fields = next(csvreader)  # header row
        for row in csvreader:
            values = "','".join(row)
            sql_command = (
                f"INSERT INTO {table} ({','.join(fields)}) VALUES ('{values}')"
            )
            cursor.execute(sql_command)
    cursor.commit()


def database_create(*, cursor=None, database=None, drop=False, setup=None):
    """Create database.

    Args:
        cursor: pyodbc cursor, connected to SQL Server database instance
        database: name of the database to be created
        drop: whether to drop any existing database of that name first
        setup: optional name of file containing SQL commands to be executed
                   after the database is created. (Typically a list of CREATE
                   TABLE commands).
    """
    if not cursor:
        raise ValueError("database_create: missing required argument (cursor)")
    if not database:
        raise ValueError("database_create: missing required argument (database)")

    if drop:
        with suppress(pyodbc.ProgrammingError):
            cursor.execute(f"DROP DATABASE {database}")

    with suppress(pyodbc.ProgrammingError):
        cursor.execute(f"CREATE DATABASE {database}")
        cursor.commit()

    if setup:
        cursor.execute(f"USE {database};")
        with open(setup, "r") as fhandle:
            for line in fhandle:
                sqlcmd = line.strip()
                if sqlcmd:
                    cursor.execute(sqlcmd)
        cursor.commit()


def database_print(*, cursor=None, database=None):
    """Print a summary of the tables in a database.
    """
    if not cursor:
        raise ValueError("database_print: missing required argument (cursor)")
    if not database:
        raise ValueError("database_print: missing required argument (database)")

    print(f"\ndatabase: {database}")
    cursor.execute(
        f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='{database}';"
    )
    tables = [_[0] for _ in cursor.fetchall()]
    for table in tables:
        print(f"{table} table: {', '.join(table_columns(cursor=cursor, table=table))}")


def table_columns(*, cursor=None, database=None, table=None):
    """Returns a list of the column names for a table.
    """
    if not cursor:
        raise ValueError("table_columns: missing required argument (cursor)")
    if not table:
        raise ValueError("table_columns: missing required argument (table)")

    if database:
        cursor.execute(f"USE {database}")

    cursor.execute(
        f"select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{table}'"
    )
    results = cursor.fetchall()
    return [result[3] for result in results]


def table_print(*, cursor=None, database=None, table=None, title=None, rows=10):
    """Print contents of a table or cursor to the console. This is for quick
    printing of small result sets for diagnostic purposes, may not work well
    with a large numbers of columns.

    Args:
        cursor: a cursor from a pyodbc connection; if table is provided, a
                SELECT * FROM <table> will be executed in this cursor; if table
                is not provided, the cursor should contain a result set (i.e.,
                the most recent SQL query was a SELECT.
        database: database name for the table (optional)
        table: name of table to be printed; if not provided, the result set
               in the cursor will be printed.
        title: optional title
        rows: maximum number of rows to print
    """
    if not cursor:
        raise ValueError("table_print: missing required argument (cursor)")

    if not title:
        title = f"{table} table" if table else "cursor"

    if table:
        columns = table_columns(cursor=cursor, table=table)
    else:
        columns = [row[0] for row in cursor.description]

    # default column width is the length of each column name
    widths = [len(column) for column in columns]

    # adjust column widths based on the data found in each column
    if table:
        if database:
            cursor.execute(f"USE {database}")
        cursor.execute(f"SELECT * FROM {table}")
    cursor_rows = cursor.fetchall()
    for ncol, col_width in enumerate(widths):
        max_data_width = max([len(str(row[ncol])) for row in cursor_rows[:rows]])
        widths[ncol] = max(col_width, max_data_width)

    # all output lines will be truncated to console_width characters
    console_width = shutil.get_terminal_size((80, 20))[0] - 1

    # print header rows
    tot_width = min(sum(widths) + len(columns) - 1, console_width)
    title_plus_rowcount = f"{title} ({len(cursor_rows)} rows)"
    print("\n" + f" {title_plus_rowcount} ".center(tot_width, "-"))
    column_headers = " ".join(
        [column.ljust(width) for column, width in zip(columns, widths)]
    )
    print(column_headers[:console_width])
    underlines = " ".join([width * "-" for width in widths])
    print(underlines[:console_width])

    # print data rows
    for row_number, row in enumerate(cursor_rows):
        printed_row = " ".join(
            [str(value).ljust(width) for value, width in zip(row, widths)]
        )
        print(printed_row[:console_width])
        if row_number >= rows - 1:
            break
