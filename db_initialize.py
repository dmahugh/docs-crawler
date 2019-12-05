"""Initialize the crawler database.
"""
import csv
import os

import pyodbc

from config import DB_USER, DB_PASS, DB_NAME

from utils import create_database, print_database, print_table


def create_jobdef(*, cursor=None, database=None):
    """Create and populate the jobdef table of job definitions.

    jobdef columns:
    job_id = key field, identifer for this job type
    start_page = URL of the page from which crawling begins
    single_domain = whether to only crawl links within startpage's domain (1=True)
    subpath = optional; if specified, crawling is limited to this subpath of the domain
    max_pages = maximum number of pages to crawl
    daily = whether to run this job type every day (1=True)
    """
    if not cursor:
        raise ValueError("print_jobdef: missing required argument (cursor)")
    if not database:
        raise ValueError("print_jobdef: missing required argument (database)")

    cursor.execute(f"USE {database}")
    cursor.execute("DROP TABLE IF EXISTS jobdef")
    cursor.execute(
        """
    CREATE TABLE jobdef (
    job_id CHAR(20) PRIMARY KEY,
    start_page VARCHAR(120),
    single_domain BIT,
    subpath VARCHAR(120),
    max_pages INT,
    daily BIT);"""
    )

    with open("jobdef.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            job_id, start_page, single_domain, subpath, max_pages, daily = row
            sql_command = f"INSERT INTO jobdef (job_id, start_page, single_domain, subpath, max_pages, daily) VALUES ('{job_id}', '{start_page}', '{single_domain}', '{subpath}', '{max_pages}', '{daily}')"
            cursor.execute(sql_command)

    cursor.commit()


def print_jobdef(*, cursor=None, database=None):
    """Print contents of the jobdef table.
    """
    if not cursor:
        raise ValueError("print_jobdef: missing required argument (cursor)")
    if not database:
        raise ValueError("print_jobdef: missing required argument (database)")

    cursor.execute("SELECT * FROM jobdef ORDER BY daily, job_id;")
    print_table(cursor=cursor, title="jobdef table", rows=99)


def main():
    """Create the crawler database and its tables.
    """

    # Connection string for Cloud SQL Proxy assumes that the proxy is running
    # locally and was invoked like this:
    # cloud_sql_proxy.exe -instances=project:region:instance=tcp:1433 -credential_file=credentials.json
    driver = "ODBC Driver 17 for SQL Server"
    proxy_addr = "127.0.0.1"
    conn_str = f"DRIVER={{{driver}}};SERVER={proxy_addr};UID={DB_USER};PWD={DB_PASS}"

    with pyodbc.connect(conn_str, autocommit=True).cursor() as cursor:
        create_database(cursor=cursor, database=DB_NAME)
        create_jobdef(cursor=cursor, database=DB_NAME)
        print_jobdef(cursor=cursor, database=DB_NAME)
        print_database(cursor=cursor, database=DB_NAME)

if __name__ == "__main__":
    main()
