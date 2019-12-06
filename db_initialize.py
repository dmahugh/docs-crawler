"""Initialize the crawler database.
"""
import csv
import os

import pyodbc

from config import DB_USER, DB_PASS, DB_NAME

from utils import database_create, database_print, table_print


def crawled_create(*, cursor=None, database=None):
    """Create and populate the crawled table of crawled pages.

    crawled columns:
    job_id = jobhist.job_id of the crawler job in which this page was crawled
    page_url = URL of the page
    crawled = PST timestamp of when the page was crawled
    """
    if not cursor:
        raise ValueError("crawled_create: missing required argument (cursor)")
    if not database:
        raise ValueError("crawled_create: missing required argument (database)")

    cursor.execute(f"USE {database}")
    cursor.execute("DROP TABLE IF EXISTS crawled")
    cursor.execute(
        """
    CREATE TABLE crawled (
    job_id INT,
    page_url VARCHAR(120),
    crawled DATETIMEOFFSET);"""
    )

    with open("initdata\\crawled.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            job_id, page_url = row
            sql_command = f"INSERT INTO crawled (job_id, page_url) VALUES ('{job_id}', '{page_url}')"
            cursor.execute(sql_command)

    cursor.commit()

    cursor.execute("SELECT job_id, page_url FROM crawled;")
    table_print(cursor=cursor, title="crawled table")


def jobdef_create(*, cursor=None, database=None):
    """Create and populate the jobdef table of job definitions.

    jobdef columns:
    jobtype = key field, identifer for this job type
    start_page = URL of the page from which crawling begins
    single_domain = whether to only crawl links within startpage's domain (1=True)
    subpath = optional; if specified, crawling is limited to this subpath of the domain
    max_pages = maximum number of pages to crawl
    daily = whether to run this job type every day (1=True)
    """
    if not cursor:
        raise ValueError("jobdef_create: missing required argument (cursor)")
    if not database:
        raise ValueError("jobdef_create: missing required argument (database)")

    cursor.execute(f"USE {database}")
    cursor.execute("DROP TABLE IF EXISTS jobdef")
    cursor.execute(
        """
    CREATE TABLE jobdef (
    jobtype CHAR(20) PRIMARY KEY,
    start_page VARCHAR(120),
    single_domain BIT,
    subpath VARCHAR(120),
    max_pages INT,
    daily BIT);"""
    )

    with open("initdata\\jobdef.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            jobtype, start_page, single_domain, subpath, max_pages, daily = row
            sql_command = f"INSERT INTO jobdef (jobtype, start_page, single_domain, subpath, max_pages, daily) VALUES ('{jobtype}', '{start_page}', '{single_domain}', '{subpath}', '{max_pages}', '{daily}')"
            cursor.execute(sql_command)

    cursor.commit()

    cursor.execute("SELECT * FROM jobdef ORDER BY daily, jobtype;")
    table_print(cursor=cursor, title="jobdef table")


def jobhist_create(*, cursor=None, database=None):
    """Create and populate the jobhist table of crawler job executions.

    jobhist columns:
    job_id = unique identifer for this job run
    jobtype = jobdef.jobtype value for this job type definition
    queued = PST datetime of when this job was added to the queue
    jobstart = PST datetime of when job execution started
    jobend = PST datetime of when job execution ended
    elapsed = total seconds of execution time
    links = total links crawled
    pages = total pages crawled
    missing = total links to missing pages
    """
    if not cursor:
        raise ValueError("jobhist_create: missing required argument (cursor)")
    if not database:
        raise ValueError("jobhist_create: missing required argument (database)")

    cursor.execute(f"USE {database}")
    cursor.execute("DROP TABLE IF EXISTS jobhist")
    cursor.execute(
        """
    CREATE TABLE jobhist (
    job_id INT IDENTITY(1,1) PRIMARY KEY,
    jobtype CHAR(20),
    queued DATETIMEOFFSET,
    jobstart DATETIMEOFFSET,
    jobend DATETIMEOFFSET,
    elapsed INT,
    links INT,
    pages INT,
    missing INT);"""
    )

    with open("initdata\\jobhist.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            jobtype, elapsed, links, pages, missing = row
            sql_command = f"INSERT INTO jobhist (jobtype, elapsed, links, pages, missing) VALUES ('{jobtype}', '{elapsed}', '{links}', '{pages}', '{missing}')"
            cursor.execute(sql_command)

    cursor.commit()

    cursor.execute("SELECT job_id, jobtype, elapsed, links, pages, missing FROM jobhist;")
    table_print(cursor=cursor, title="jobhist table")


def notfound_create(*, cursor=None, database=None):
    """Create and populate the notfound table of "Page Not Found" links.

    notfound columns:
    job_id = the jobhist.job_id value for this crawler job
    found = PST datetime when this broken link was found
    source = the page linked from
    target = the page linked to
    link_text = the display text of the link on the source page
    """
    if not cursor:
        raise ValueError("notfound_create: missing required argument (cursor)")
    if not database:
        raise ValueError("notfound_create: missing required argument (database)")

    cursor.execute(f"USE {database}")
    cursor.execute("DROP TABLE IF EXISTS notfound")
    cursor.execute(
        """
    CREATE TABLE notfound (
    job_id INT,
    found DATETIMEOFFSET,
    source VARCHAR(120),
    target VARCHAR(120),
    link_text VARCHAR(120));"""
    )

    with open("initdata\\notfound.csv", "r") as csvfile:
        csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        next(csvreader)  # skip header row
        for row in csvreader:
            job_id, source, target, link_text = row
            sql_command = f"INSERT INTO notfound (job_id, source, target, link_text) VALUES ('{job_id}', '{source}', '{target}', '{link_text}')"
            cursor.execute(sql_command)

    cursor.commit()

    cursor.execute("SELECT job_id, source, target, link_text FROM notfound;")
    table_print(cursor=cursor, title="notfound table")


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
        database_create(cursor=cursor, database=DB_NAME)
        jobdef_create(cursor=cursor, database=DB_NAME)
        jobhist_create(cursor=cursor, database=DB_NAME)
        notfound_create(cursor=cursor, database=DB_NAME)
        crawled_create(cursor=cursor, database=DB_NAME)


if __name__ == "__main__":
    main()
