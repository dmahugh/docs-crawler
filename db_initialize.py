"""Initialize the crawler database.
Assumes Cloud SQL Proxy is running locally and listening on 127.0.0.1.
"""
import csv
import os

import pyodbc

from config import DB_USER, DB_PASS, DB_NAME, ODBC_DRIVER, PROXY_ADDR

from utils import csv_insert, database_create, table_print


def main():
    """Create the crawler database and its tables.
    """

    # open a connection to Cloud SQL
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};SERVER={PROXY_ADDR};UID={DB_USER};PWD={DB_PASS}"
    )
    with pyodbc.connect(conn_str, autocommit=True).cursor() as cursor:

        # create the database
        database_create(cursor=cursor, database=DB_NAME, drop=True)

        # create the tables
        sql_script = f"""USE {DB_NAME};
CREATE TABLE jobdef (jobtype CHAR(20) PRIMARY KEY, start_page VARCHAR(120),
                     single_domain BIT, subpath VARCHAR(120), max_pages INT,
                     daily BIT);
CREATE TABLE jobhist (job_id INT IDENTITY(1,1) PRIMARY KEY, jobtype CHAR(20),
                      queued DATETIME2, jobstart DATETIME2, jobend DATETIME2,
                      elapsed INT, links INT, pages INT, missing INT);
CREATE TABLE crawled (job_id INT, page_url VARCHAR(120), crawled DATETIME2);
CREATE TABLE notfound (job_id INT, found DATETIME2, source VARCHAR(120),
                       target VARCHAR(120), link_text VARCHAR(120));"""
        cursor.execute(sql_script)
        cursor.commit()

        # load sample data from CSV files into each of the tables
        for table in ["jobdef", "jobhist", "crawled", "notfound"]:
            csv_insert(
                cursor=cursor,
                database=DB_NAME,
                table=table,
                filename=f"initdata\\{table}.csv",
            )
            table_print(cursor=cursor, table=table)


if __name__ == "__main__":
    main()
