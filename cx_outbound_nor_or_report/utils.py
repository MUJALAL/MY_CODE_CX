from datetime import datetime
import pandas as pd
import psycopg2
import os
import sys
import time
import yaml
from datetime import timedelta

# Redshift upload libraries
from pandas.io.sql import SQLTable
from sqlalchemy import create_engine

def _execute_insert(self, conn, keys, data_iter):
    print("Using monkey-patched _execute_insert")
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(self.table.insert().values(data))


SQLTable._execute_insert = _execute_insert


# PROJECT_CONFIG_PATH = './../../config.yaml'


def read_sql_file(path):
    fd = open(path, 'r')
    sql_file = fd.read()
    fd.close()
    return sql_file


def fetch_data(query, connection):
#     dir_name = os.path.dirname(os.path.abspath(__file__))
#     credentials = yaml.safe_load(open(os.path.join(dir_name, PROJECT_CONFIG_PATH)))
    dir_name = os.path.dirname(os.path.abspath(__file__))
    credentials = yaml.safe_load(open(os.path.join(dir_name , 'config.yaml')))
    # credentials = yaml.safe_load(open('../config.yaml'))
    
    db_credentials = credentials['databases'][connection]
    query_start_ts = time.time()
    no_result = True
    conn = psycopg2.connect(**db_credentials)
    counter = 0
    while no_result:
        if counter < 5:    
            try:
                counter += 1
                df = pd.read_sql_query(query, conn)
                no_result = False
            except psycopg2.Error as error:
                print('There was an error with the database operation: {}'.format(error))
                conn = psycopg2.connect(**db_credentials)
            except Exception as e:
                print('There was an unexpected error of type {}'.format(e))
                conn = psycopg2.connect(**db_credentials)
            finally:
                conn.close()
        else:
            break

    seconds_taken = time.time() - query_start_ts
    print('Query run time in seconds : {}'.format(seconds_taken))
    return df


def __get_sqlalchemy_engine():
    
#     print('el')
    dir_name = os.path.dirname(os.path.abspath(__file__))
    credentials = yaml.safe_load(open(os.path.join(dir_name , 'config.yaml')))
    
    # credentials = yaml.safe_load(open('config.yaml'))

    database = credentials['databases']['redshift_db']['database']
    username = credentials['databases']['redshift_db']['user']
    host = credentials['databases']['redshift_db']['host']
    password = credentials['databases']['redshift_db']['password']
    port = credentials['databases']['redshift_db']['port']

    engine_query = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}".format(
        username = username,
        password=password,
        host=host,
        port=port,
        database=database
    )
    engine = create_engine(engine_query, echo = False)

    return engine


def write_to_redshift(table_name, dataframe):
    query_start_ts = time.time()
    redshift_engine = __get_sqlalchemy_engine()
#     print(1)
#     dataframe['created_at_redshift'] = datetime.now() + timedelta(hours=5.5)
    dataframe.to_sql(
        table_name, redshift_engine, if_exists = 'append',
        chunksize = 1000, index = False
    )
    seconds_taken = time.time() - query_start_ts
    print('Query run time in seconds: {}'.format(seconds_taken))
    
