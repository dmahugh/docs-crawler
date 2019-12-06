"""Initialize the crawler database.
Assumes Cloud SQL Proxy is running locally and listening on 127.0.0.1.
"""
import csv
import os

import pyodbc

from config import DB_USER, DB_PASS, DB_NAME, ODBC_DRIVER, PROXY_ADDR

from utils import csv_insert, database_create, database_print, table_print


def main():
    """Create the crawler database and its tables.
    """

    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};SERVER={PROXY_ADDR};UID={DB_USER};PWD={DB_PASS}"
    )
    with pyodbc.connect(conn_str, autocommit=True).cursor() as cursor:
        database_create(
            cursor=cursor, database=DB_NAME, drop=True, setup="initdata\\db_setup.sql"
        )
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
