"""Initialize the crawler database and jobdef table.

This script runs locally against the cloud SQL proxy, which should be invoked
with this command:
cloud_sql_proxy.exe -instances=project:region:instance=tcp:1433 -credential_file=credentials.json

"""
from contextlib import suppress
import os
import shutil

import pyodbc

from config import DB_USER, DB_PASS, DB_NAME

CNXN = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=127.0.0.1;"
    + f"UID={DB_USER};PWD={DB_PASS}",
    autocommit=True,
)
CURSOR = CNXN.cursor()


def create_database(db_name):
    """Create database.
    """
    with suppress(pyodbc.ProgrammingError):
        CURSOR.execute(f"CREATE DATABASE {db_name}")
        CNXN.commit()

def create_jobdef(db_name):
    """Create and populate the jobdef table of job definitions.
    """
    CURSOR.execute(f"USE {db_name}")
    CURSOR.execute("DROP TABLE IF EXISTS jobdef")

    sql_command = """
    CREATE TABLE jobdef (
    job_id CHAR(20) PRIMARY KEY,
    start_page VARCHAR(120),
    single_domain BIT,
    subpath VARCHAR(120),
    max_pages INT,
    active BIT);"""
    CURSOR.execute(sql_command)

    job_id = "Cloud SQL docs"
    start_page = "https://cloud.google.com/sql/docs/"
    single_domain = 1
    subpath = "/sql/docs"
    max_pages = 10
    active = 1
    sql_command = f"INSERT INTO jobdef (job_id, start_page, single_domain, subpath, max_pages, active) VALUES ('{job_id}', '{start_page}', '{single_domain}', '{subpath}', '{max_pages}', '{active}')"
    CURSOR.execute(sql_command)

    CNXN.commit()


def column_names(cursor, table_name):
    """Returns a list of the column names for a table.
    """
    cursor.execute(f"USE {DB_NAME}")
    cursor.execute(f"exec sp_columns '{table_name}'")
    results = cursor.fetchall()
    return [result[1] for result in results]


def print_jobdef(db_name):
    """Print contents of the jobdef table.
    """

    CURSOR.execute("SELECT job_id, start_page FROM jobdef;")
    print_table(cursor=CURSOR, title="jobdef table", rows=99)


def print_database(db_name):
    """Print a summary of the tables in a database file.
    """
    print(f"{db_name}")
    CURSOR.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [_[0] for _ in CURSOR.fetchall() if not _[0].startswith("sqlite_")]
    for table in tables:
        print(f"+- {table} ({','.join(column_names(CURSOR, table))})")


def print_table(*, db_name=None, cursor=None, table=None, title=None, rows=10):
    """Print contents of a table or cursor.
    This is for quick printing of small data sets for diagnostic purposes, may
    not work well with large numbers of columns.
    """
    if not cursor and not (db_name and table):
        raise ValueError("print_table requires cursor or db_name/table")

    if not title:
        title = "cursor" if cursor else f"{table} table"

    if cursor:
        columns = [row[0] for row in cursor.description]
    else:
        cursor = CURSOR
        columns = column_names(cursor, table)

    # default column width is the length of each column name
    widths = [len(column) for column in columns]

    # adjust column widths based on the data found in each column
    if db_name and table:
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


def main():
    """Create an acme_corp database and populate some tables with random data
    """
    create_database(DB_NAME)
    create_jobdef(DB_NAME)
    print_jobdef(DB_NAME)


if __name__ == "__main__":
    main()
