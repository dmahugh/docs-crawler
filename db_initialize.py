"""Initialize the crawler database.
"""
import csv
import os

import pyodbc

from config import DB_USER, DB_PASS, DB_NAME

from utils import database_create, database_print, table_print


def crawled_create(*, cursor=None, database=None):
    """Create and populate the crawled table of crawled pages.
    """
    if not cursor:
        raise ValueError("crawled_create: missing required argument (cursor)")
    if not database:
        raise ValueError("crawled_create: missing required argument (database)")

    cursor.execute(f"USE {database}")
    with open("initdata\\crawled.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            job_id, page_url, crawled = row
            sql_command = f"INSERT INTO crawled (job_id, page_url, crawled) VALUES ('{job_id}', '{page_url}', '{crawled}')"
            cursor.execute(sql_command)

    cursor.commit()

    table_print(cursor=cursor, table="crawled")


def jobdef_create(*, cursor=None, database=None):
    """Create and populate the jobdef table of job definitions.
    """
    if not cursor:
        raise ValueError("jobdef_create: missing required argument (cursor)")
    if not database:
        raise ValueError("jobdef_create: missing required argument (database)")

    cursor.execute(f"USE {database}")
    with open("initdata\\jobdef.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            jobtype, start_page, single_domain, subpath, max_pages, daily = row
            sql_command = f"INSERT INTO jobdef (jobtype, start_page, single_domain, subpath, max_pages, daily) VALUES ('{jobtype}', '{start_page}', '{single_domain}', '{subpath}', '{max_pages}', '{daily}')"
            cursor.execute(sql_command)

    cursor.commit()

    table_print(cursor=cursor, table="jobdef")


def jobhist_create(*, cursor=None, database=None):
    """Create and populate the jobhist table of crawler job executions.
    """
    if not cursor:
        raise ValueError("jobhist_create: missing required argument (cursor)")
    if not database:
        raise ValueError("jobhist_create: missing required argument (database)")

    cursor.execute(f"USE {database}")
    with open("initdata\\jobhist.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            jobtype, elapsed, links, pages, missing = row
            sql_command = f"INSERT INTO jobhist (jobtype, elapsed, links, pages, missing) VALUES ('{jobtype}', '{elapsed}', '{links}', '{pages}', '{missing}')"
            cursor.execute(sql_command)

    cursor.commit()

    table_print(cursor=cursor, table="jobhist")


def notfound_create(*, cursor=None, database=None):
    """Create and populate the notfound table of "Page Not Found" links.
    """
    if not cursor:
        raise ValueError("notfound_create: missing required argument (cursor)")
    if not database:
        raise ValueError("notfound_create: missing required argument (database)")

    cursor.execute(f"USE {database}")
    with open("initdata\\notfound.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            job_id, source, target, link_text = row
            sql_command = f"INSERT INTO notfound (job_id, source, target, link_text) VALUES ('{job_id}', '{source}', '{target}', '{link_text}')"
            cursor.execute(sql_command)

    cursor.commit()

    table_print(cursor=cursor, table="notfound")


def main():
    """Create the crawler database and its tables.
    """

    # Connection string for Cloud SQL Proxy assumes that the proxy is running
    # locally and was invoked like this:
    # cloud_sql_proxy.exe -instances=project:region:instance=tcp:1433 -credential_file=credentials.json
    driver = "SQL Server Native Client 11.0"
    proxy_addr = "127.0.0.1"
    conn_str = f"DRIVER={{{driver}}};SERVER={proxy_addr};UID={DB_USER};PWD={DB_PASS}"

    with pyodbc.connect(conn_str, autocommit=True).cursor() as cursor:
        database_create(cursor=cursor, database=DB_NAME, drop=True, setup="initdata\\db_setup.sql")
        jobdef_create(cursor=cursor, database=DB_NAME)
        jobhist_create(cursor=cursor, database=DB_NAME)
        notfound_create(cursor=cursor, database=DB_NAME)
        crawled_create(cursor=cursor, database=DB_NAME)


if __name__ == "__main__":
    main()
