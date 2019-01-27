import time
import os
import argparse
import logging
import json
import pandas as pd
from sqlalchemy import create_engine
import urllib
import pyodbc


logging.info(pyodbc.drivers())
print(f'Using ----> {pyodbc.drivers()}')
driver = pyodbc.drivers()[0]

# default args
sql_path = "/home/vitorc/Documents/banco-grego.json"
if_table_exist = 'replace'  # other options include append



def connection(path):
    sql_auth = path
    with open(sql_auth) as f:
        sql_info = json.load(f)
    return sql_info

def conn_parser(db_info):
    params = urllib.parse.quote_plus(
        'DRIVER={0};\
        SERVER={1};\
        DATABASE={2};UID={3};PWD={4}'.format(
            driver, db_info['host'], db_info['db_name'],
        db_info['user'], db_info['password'])
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"


def main(df, table_name, sql_path=sql_path, if_table_exist=if_table_exist):

    db_info = connection(sql_path)
    sql_engine = create_engine(conn_parser(db_info))

    s = time.time()
    df.to_sql(table_name, sql_engine, if_exists=if_table_exist, chunksize=None, index=False)
    print(f'Ingestion total time was ->> {time.time() - s}')


