import psycopg2
from sqlalchemy import create_engine, event
import time
import pandas as pd
import os
import datetime
from datetime import timedelta
import yaml
import os


    
def read_sql_file(path):
    fd = open(path, 'r')
    sql_file = fd.read()
    fd.close()
    return sql_file


def fetch_data(query, connection):
    dir_name = os.path.dirname(os.path.abspath(__file__))
    credentials = yaml.safe_load(open('config.yaml'))
#     credentials = yaml.safe_load(open(os.path.join(dir_name,"./../../config.yaml")))
    db_credentials = credentials['databases'][connection]
    query_start_ts = time.time()
    no_result = True
    while no_result:
        try:
            conn = psycopg2.connect(**db_credentials)
            df = pd.read_sql_query(query, conn)
            no_result = False
        except psycopg2.Error as error:
            print('There was an error with the database operation: \
                {}'.format(error))
            conn.close()
        except Exception as e:
            print('There was an unexpected error of type \
                {}'.format(e))
            conn.close()
        finally:
            conn.close()
    seconds_taken = time.time() - query_start_ts
    print('Query run time in seconds : {}'.format(seconds_taken))
    return df

def __get_sqlalchemy_engine():
    dir_name = os.path.dirname(os.path.abspath(__file__))
#     credentials = yaml.safe_load(open(os.path.join(dir_name, "./../../config.yaml")))
    credentials = yaml.safe_load(open('config.yaml'))

    database = credentials['databases']['redshift_db']['database']
    username = credentials['databases']['redshift_db']['user']
    host = credentials['databases']['redshift_db']['host']
    password = credentials['databases']['redshift_db']['password']
    port = credentials['databases']['redshift_db']['port']

    engine_query = "postgresql+psycopg2://{username}:{password}@{host}:\
        {port}/{database}".format(
        username = username,
        password=password,
        host=host,
        port=port,
        database=database
    )
    engine = create_engine(engine_query, echo = False)
    return engine

def write_to_redshift(table_name, dataframe):
    redshift_engine = __get_sqlalchemy_engine()
#     dataframe['created_at'] = datetime.now()
    dataframe['created_at_redshift'] = datetime.datetime.now() + timedelta(hours=5.5)
    dataframe.to_sql(
        table_name, redshift_engine, if_exists = 'append',
        chunksize = 1000, index = False)
