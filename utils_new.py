import psycopg2
from sqlalchemy import create_engine, event
import time
import pandas as pd
import datetime
from datetime import timedelta

def get_sqlalchemy_engine():
    engine_query = (f"postgresql+psycopg2://anant:Resfeber123@commons-prod-redshift.porter.in:5439/porter")
    engine = create_engine(engine_query, echo = False)
    return engine


def write_to_redshift_append(table_name,  dataframe,  engine):
    print("in append")
    query_start_ts = time.time()
    dataframe['created_at_redshift'] = datetime.datetime.now() + timedelta(hours=5.5)
    dataframe.to_sql(table_name,  engine,  if_exists = 'append', schema='anant',  chunksize = 1000,  index = False)
    seconds_taken = time.time() - query_start_ts
    print('Query run time in seconds: {}'.format(seconds_taken))
    
def read_sql_file(path):
    fd = open(path, 'r')
    sql_file = fd.read()
    fd.close()
    return sql_file

def fetch_data(creds, query, connection, params=None):
    db_creds = creds['databases'][connection]
    query_start_ts = time.time()
    no_result = True
    while no_result:
        try:
            conn = psycopg2.connect(**db_creds)
            df = pd.read_sql_query(query, conn, params)
            no_result = False
        except psycopg2.Error as error:
            print('There was an error with the database operation: {}'.format(error))
        finally:
            conn.close()
        seconds_taken = time.time() - query_start_ts
        print('Query run time in seconds: {}'.format(seconds_taken))
        return df
