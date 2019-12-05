"""Initialize the crawler database and jobdef table.

This script runs locally against the cloud SQL proxy, which should be invoked
with this command:
cloud_sql_proxy.exe -instances=project:region:instance=tcp:1433 -credential_file=credentials.json

"""
from contextlib import suppress
import os
import shutil

from faker import Faker

import pyodbc

LOCATIONS = 8  # number of locations
EMPLOYEES = 4  # number of employees per location

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
    try:
        CURSOR.execute(f"CREATE DATABASE {db_name}")
        CNXN.commit()
    except:
        pass  # need to figure out how to use IF DB_ID() from pyodbc


def generate_employees(db_name):
    """Create employee table and populate it with sample data.
    """
    CURSOR.execute(f"USE {db_name}")
    CURSOR.execute("DROP TABLE IF EXISTS employee")

    sql_command = """
    CREATE TABLE employee (
    emp_id INT IDENTITY(1,1) PRIMARY KEY,
    first_name VARCHAR(40),
    last_name VARCHAR(40),
    title VARCHAR(80),
    office_id INT,
    pwd CHAR(15),
    ipaddr CHAR(15),
    ssn CHAR(11));"""
    CURSOR.execute(sql_command)

    fake = Faker()
    for office_id in range(1, LOCATIONS + 1):
        # generate some random employees for each office location
        for _ in range(EMPLOYEES):
            first_name = fake.first_name()  # pylint: disable=no-member
            last_name = fake.last_name()  # pylint: disable=no-member
            jobtitle = fake.job().split("/")[0]  # pylint: disable=no-member
            pwd = fake.password()  # pylint: disable=no-member
            ipaddr = fake.ipv4()  # pylint: disable=no-member
            ssn = fake.ssn()  # pylint: disable=no-member
            sql_command = (
                "INSERT INTO employee "
                "(first_name, last_name, title, office_id, pwd, ipaddr, ssn) "
                f"VALUES ('{first_name}', '{last_name}', '{jobtitle}', "
                f"{office_id}, '{pwd}', '{ipaddr}', '{ssn}')"
            )
            CURSOR.execute(sql_command)

    CNXN.commit()


def generate_locations(db_name):
    """Create location table and populate it with sample data.
    """
    CURSOR.execute(f"USE {db_name}")
    CURSOR.execute("DROP TABLE IF EXISTS location")

    sql_command = """
    CREATE TABLE location (
    office_id INT IDENTITY(1,1) PRIMARY KEY,
    address VARCHAR(80),
    city VARCHAR(40),
    state CHAR(2));"""
    CURSOR.execute(sql_command)

    fake = Faker()
    for _ in range(LOCATIONS):
        address = fake.street_address()  # pylint: disable=no-member
        city = fake.city()  # pylint: disable=no-member
        state = fake.state_abbr()  # pylint: disable=no-member
        sql_command = f"INSERT INTO location (address, city, state) VALUES ('{address}', '{city}', '{state}')"
        CURSOR.execute(sql_command)

    CNXN.commit()


def column_names(cursor, table_name):
    """Returns a list of the column names for a table.
    """
    cursor.execute(f"USE {DB_NAME}")
    cursor.execute(f"exec sp_columns '{table_name}'")
    results = cursor.fetchall()
    return [result[1] for result in results]


def company_roster(db_name):
    """Create and print a company roster from the employee and location tables.
    """

    CURSOR.execute(
        """SELECT location.city, location.state, employee.first_name,
    employee.last_name, employee.title
    FROM employee
    INNER JOIN location ON employee.office_id=location.office_id;"""
    )
    print_table(cursor=CURSOR, title="Company Roster", rows=99)


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
    generate_locations(DB_NAME)
    generate_employees(DB_NAME)
    company_roster(DB_NAME)


if __name__ == "__main__":
    main()
