"""Initialize the crawler database.
"""
import csv
import os

import pyodbc

from config import DB_USER, DB_PASS, DB_NAME

from utils import csv_insert, database_create, database_print, table_print


def main():
    """Create the crawler database and its tables.
    """

    driver = "SQL Server Native Client 11.0"
    proxy_addr = "127.0.0.1" # assumes Cloud SQL Proxy is running locally
    conn_str = f"DRIVER={{{driver}}};SERVER={proxy_addr};UID={DB_USER};PWD={DB_PASS}"
    with pyodbc.connect(conn_str, autocommit=True).cursor() as cursor:

        database_create(cursor=cursor, database=DB_NAME, drop=True, setup="initdata\\db_setup.sql")

        csv_insert(cursor=cursor, database=DB_NAME, table="jobdef", filename="initdata\\jobdef.csv")
        csv_insert(cursor=cursor, database=DB_NAME, table="jobhist", filename="initdata\\jobhist.csv")
        csv_insert(cursor=cursor, database=DB_NAME, table="crawled", filename="initdata\\crawled.csv")
        csv_insert(cursor=cursor, database=DB_NAME, table="notfound", filename="initdata\\notfound.csv")

        table_print(cursor=cursor, table="jobdef")
        table_print(cursor=cursor, table="jobhist")
        table_print(cursor=cursor, table="crawled")
        table_print(cursor=cursor, table="notfound")

if __name__ == "__main__":
    main()
