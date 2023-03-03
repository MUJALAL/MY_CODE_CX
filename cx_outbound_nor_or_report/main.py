import warnings
warnings.filterwarnings('ignore')

import yaml
import time
from datetime import datetime as dt, timedelta
# Dataframe manipulation
import pandas as pd
import numpy as np

# import plotly.express as px
# Maths
import math

# DB related
import psycopg2

from itertools import repeat

from functools import reduce
from pandas.tseries.offsets import MonthBegin
from sqlalchemy import create_engine, event

import utils, os
pd.set_option('display.max_columns', 1000)
pd.options.display.float_format = '{:.2f}'.format ## to remove exponential

dir_name = os.path.dirname(os.path.abspath(__file__))

# Fetching Data

def fetch_data():

    startdate = '2023-03-01'

    # Fetching the max date from the redshift table 

    # date = utils.read_sql_file('queries/date.sql')
    # startdate = utils.fetch_data(date, 'redshift_db')
    # startdate = startdate['date'][0].date().strftime('%Y-%m-%d')

    print("The datapull would start from {} \n".format(startdate))

    # Fetching Requests Details From Outbound_Requests_Logs Table
    
    outbound_request_query = utils.read_sql_file(os.path.join(dir_name,'queries/outbound_request_query.sql')).format(startdate = startdate)
    outbound_request = utils.fetch_data(outbound_request_query, 'oms_prod_snapshot')

    print("outbound request table fetched succesfully....\n")

    # Fetching Details From Outbound_Call_Disposition Which Helps to join table between outbound_req_logs with Calls_Table
    
    mapping_for_outbound_to_request_query = utils.read_sql_file(os.path.join(dir_name,'queries/mapping_for_outbound_to_request_query.sql')).format(request_id = tuple(set(outbound_request.id)))
    mapping_for_outbound_to_request = utils.fetch_data(mapping_for_outbound_to_request_query, 'oms_prod_snapshot')

    print("outbound call disposition table fetched succesfully....\n")


    # Fetching Calls Details From Disposition, Abondoned & Caller_responses Table with campaign_id = 38
    
    outbound_calls_query = utils.read_sql_file(os.path.join(dir_name,'queries/outbound_calls_query.sql')).format(startdate = startdate, disposed_call_id = tuple(mapping_for_outbound_to_request.disposed_call_id.unique()))
    outbound_calls_df = utils.fetch_data(outbound_calls_query, 'oms_prod_snapshot')

    print("outbound calls table fetched succesfully....\n")


    # Fetching Additional Details from caller_responses such as (City, queue_name, ivr and so on)
    
    city_vicinity_query = utils.read_sql_file(os.path.join(dir_name,'queries/vicinity_geo_dets_query.sql')).format(sourceid = tuple(set(outbound_request.source_id)))
    city_vicinity_df = utils.fetch_data(city_vicinity_query, 'oms_prod_snapshot')
    
    print("outbound city viciity table fetched succesfully....\n")

    return outbound_request, mapping_for_outbound_to_request, outbound_calls_df, city_vicinity_df


# Modification of some data in the tables we've fetched above

def transforming_data(outbound_request, outbound_calls_df, city_vicinity_df):


  # adding date column in request_table and vicinity column based on order_call_stage

    outbound_request['requested_time_ist'] = pd.to_datetime(outbound_request.requested_time_ist)
    outbound_request['date'] = outbound_request.requested_time_ist.dt.date
    outbound_request['date'] = pd.to_datetime(outbound_request['date'])
    outbound_request['hour'] = outbound_request.requested_time_ist.dt.hour

    outbound_request['vicinity'] = np.where(outbound_request.order_call_stage.str.contains('accept', case = False), 'accept',
                                    np.where(outbound_request.order_call_stage.str.contains('transit', case = False),'transit', 
                                    np.where(outbound_request.order_call_stage.str.contains('start', case = False), 'start',
                                    np.where(outbound_request.order_call_stage.str.contains('drop', case = False), 'drop', 
                                    np.where(outbound_request.order_call_stage.str.contains('end', case = False), 'end', 
                                    np.where(outbound_request.order_call_stage.str.contains('nor', case = False),'nor',
                                    np.where(outbound_request.order_call_stage.str.contains('unknown', case = False),'unable_to_fetch_location', 'other')))))))

    # getting only required columns from outbound request table

    outbound_request_req_col_df = outbound_request[['id',  'mobile', 'status',
                                            'status_reason', 'created_at', 
                                            'order_call_stage', 'source_type', 'source_id', 
                                            'requested_time_ist', 'epoch_requested_time', 'date', 'hour', 'vicinity']]


   # getting only required columns from outbound calls table

    outbound_calls_require_cols_df = outbound_calls_df[['dial_time', 'disposed_call_id', 'system_disposition', 'hangup_details', 
                                                      'dialed_epoch_ts',  'call_date', 'user_id','dialed_time_ist', 'call_type']]


    # adding all the required column for Filters

    city_vicinity_df.loc[(city_vicinity_df.order_id.isna()) & (city_vicinity_df.caller_type == 1), 'vehicle_id'] = city_vicinity_df.driver_vehicle

    city_vicinity_df['ivr'] = city_vicinity_df['ivr'].fillna('ivr_null')

    city_vicinity_df['dtmf_inputs'] = city_vicinity_df['dtmf_inputs'].fillna('NoDTMFFound')

    city_vicinity_df.loc[city_vicinity_df.dtmf_inputs == '', 'dtmf_inputs'] = 'NoDTMFFound'

    city_vicinity_df['call_raisedby'] = np.where (city_vicinity_df.caller_type == 0, 'Unknown',
                                np.where (city_vicinity_df.caller_type == 1, 'Driver', 
                                np.where (city_vicinity_df.caller_type == 2, 'Customer', 'ND')))

    city_vicinity_df['order_status'] =  np.where(city_vicinity_df.status.isna(), 'NOR',
                                np.where(city_vicinity_df.status == 4, 'Completed', 'Cancelled'))

    city_vicinity_df['call_type_modified'] = np.where(city_vicinity_df.order_id.isna(), 'NOR', 
                                    np.where(city_vicinity_df.order_id.notna(), 'OR', 'ND'))

    city_vicinity_df['vehicle_name'] =  np.where(city_vicinity_df.vehicle_id.isna(), 'ND',
                                np.where(city_vicinity_df.vehicle_id == 97, '2w',  'Trucks'))

    ln = len(city_vicinity_df.crn[0])
    ln = ln - 3
    str_new = 'CRN'
    for i in range(ln):
        str_new = str_new + '0'

    city_vicinity_df.loc[city_vicinity_df.crn.isna(), 'crn'] = str_new

    # getting only required columns from city_civinty_df 

    city_vicinity_req_cols_df = city_vicinity_df[['source_id', 'driver_id', 'queue_name', 'ivr', 'city', 'channel', 'vehicle_id', 
                                        'dtmf_inputs',  'call_raisedby',
                                        'order_status', 'call_type_modified', 'vehicle_name', 'order_id', 'crn']]


    return outbound_request_req_col_df, outbound_calls_require_cols_df, outbound_request,  city_vicinity_req_cols_df


# Merging all the fetched data to get final summary

def merging(outbound_request_req_col_df, mapping_for_outbound_to_request,  outbound_calls_require_cols_df, city_vicinity_req_cols_df):

    ob_req = outbound_request_req_col_df.merge(
                                mapping_for_outbound_to_request, on = ['id'], how = 'left').merge(
                                    outbound_calls_require_cols_df, on = ['disposed_call_id'], how = 'left')


    outbound_request1 = ob_req.merge(city_vicinity_req_cols_df, on = ['source_id'], how = 'left')


    # giving a column 'flag' for unique request which doesn't contain status = [2,5]

    outbound_request1['flag'] = np.where(~(outbound_request1.status.isin([2,5])), 1, 0)


    # giving a column 'attempted' with all the disposition except failure disposition to be consider as attempted call

    attempted_system_disposition = ['CONNECTED', 'CALL_DROP', 'CALL_NOT_PICKED', 'BUSY', 'CALL_HANGUP']

    outbound_request1['attempted'] = np.where((outbound_request1.system_disposition.isin(attempted_system_disposition) | (
                                            (outbound_request1.system_disposition == 'CALL_HANGUP') & 
                                            (outbound_request1.hangup_details == 'CUSTOMER_HANGUP_PHONE'))), 1, 0)

    outbound_request1.sort_values(by = ['epoch_requested_time', 'dialed_time_ist'], inplace= True)

    # giving a column 'call_rank' to identify first, second, third dialed for each requests
    
    outbound_request1['call_rank'] = outbound_request1.groupby('id')['dialed_time_ist'].rank('dense')

    outbound_request1['connected_attempts'] = np.where(outbound_request1.system_disposition == 'CONNECTED', outbound_request1.call_rank, 0)

    outbound_request_final_df = outbound_request1.copy()

    return outbound_request_final_df


# Getting Summary Data

def summarydf(outbound_request_final_df):

    summary_col_ls = ['date', 'city', 'ivr', 'vicinity', 'call_type_modified', 'order_status', 'vehicle_name','call_raisedby', 'vehicle_id']

    summary = outbound_request_final_df.groupby(summary_col_ls,
                as_index= False)['id'].nunique().rename(columns = {'id' : 'total_requests'}).merge(
                                        
                    outbound_request_final_df[outbound_request_final_df.flag == 1].groupby(summary_col_ls, 
                            as_index= False)['id'].nunique().rename(columns = {'id' : 'unique_requests'}), 
                                        on = summary_col_ls, how = 'left').merge(
        
                    outbound_request_final_df[(outbound_request_final_df.flag == 1) & 
                                            (outbound_request_final_df.disposed_call_id.notna())].groupby(summary_col_ls, 
                            as_index= False)['id'].nunique().rename(columns = {'id' : 'unique_requests_dialed'}), 
                                        on = summary_col_ls, how = 'left').merge(
        
                    outbound_request_final_df[(outbound_request_final_df.flag == 1) & (outbound_request_final_df.disposed_call_id.notna()) 
                                                    & (outbound_request_final_df.attempted == 1)].groupby(summary_col_ls, 
                            as_index= False)['id'].nunique().rename(columns = {'id' : 'unique_requests_attempted'}), 
                                        on = summary_col_ls, how = 'left').merge(
        
                    outbound_request_final_df[(outbound_request_final_df.flag == 1) & (outbound_request_final_df.disposed_call_id.notna()) 
                                                    & (outbound_request_final_df.system_disposition == 'CONNECTED')].groupby(summary_col_ls, 
                            as_index= False)['id'].nunique().rename(columns = {'id' : 'unique_requests_connected'}), 
                                        on = summary_col_ls, how = 'left').merge(
        
                    outbound_request_final_df[outbound_request_final_df.flag == 1].groupby(summary_col_ls, 
                            as_index= False)['mobile'].nunique().rename(columns = {'mobile' : 'unique_mobile_numbers_requested'}), 
                                        on = summary_col_ls, how = 'left').merge(
            
                    outbound_request_final_df[(outbound_request_final_df.flag == 1) & 
                                            (outbound_request_final_df.disposed_call_id.notna())].groupby(summary_col_ls, 
                            as_index= False)['mobile'].nunique().rename(columns = {'mobile' : 'unique_mobile_numbers_dialed'}), 
                                        on = summary_col_ls, how = 'left').merge(
        
                    outbound_request_final_df[(outbound_request_final_df.flag == 1) & (outbound_request_final_df.disposed_call_id.notna()) 
                                                    & (outbound_request_final_df.attempted == 1)].groupby(summary_col_ls, 
                            as_index= False)['mobile'].nunique().rename(columns = {'mobile' : 'unique_mobile_numbers_attempted'}), 
                                        on = summary_col_ls, how = 'left').merge(
        
                    outbound_request_final_df[(outbound_request_final_df.flag == 1) & (outbound_request_final_df.disposed_call_id.notna()) 
                                                    & (outbound_request_final_df.system_disposition == 'CONNECTED')].groupby(summary_col_ls, 
                            as_index= False)['mobile'].nunique().rename(columns = {'mobile' : 'unique_mobile_numbers_connected'}), 
                                        on = summary_col_ls, how = 'left')

    summary['date'] = summary.date.dt.date

    return summary


# Getting Tat Data

def tatdf(outbound_request_final_df):

    # Defining List 

    id_tat_lst = ['date', 'id', 'hour', 'city', 'ivr', 'vicinity', 'call_type_modified', 'order_status', 'vehicle_name','call_raisedby', 'vehicle_id']


    # FIRST_SECOND_THIRD_DIALED_TAT

    def dialed_tat(outbound_request_final_df):


        first_dialed_time = outbound_request_final_df[(outbound_request_final_df.flag == 1) & (outbound_request_final_df.call_rank==1)].groupby(id_tat_lst
                                                , as_index = False).agg(
                                    {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'first_dialed_time'})

        first_dialed_time['first_dialed_tat'] = first_dialed_time.first_dialed_time - first_dialed_time.epoch_requested_time


        second_dialed_time = outbound_request_final_df[(outbound_request_final_df.flag == 1) & (outbound_request_final_df.call_rank==2)].groupby(id_tat_lst
                                                , as_index = False).agg(
                                {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'second_dialed_time'})

        second_dialed_time['second_dialed_tat'] = second_dialed_time.second_dialed_time - second_dialed_time.epoch_requested_time


        third_dialed_time = outbound_request_final_df[(outbound_request_final_df.flag == 1) & (outbound_request_final_df.call_rank==3)].groupby(id_tat_lst
                                                        , as_index = False).agg(
                                    {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'third_dialed_time'})

        third_dialed_time['third_dialed_tat'] = third_dialed_time.third_dialed_time - third_dialed_time.epoch_requested_time


        fst_dialed_tat = first_dialed_time[id_tat_lst + ['first_dialed_tat']].merge(
                            second_dialed_time[id_tat_lst + ['second_dialed_tat']], on = id_tat_lst, how = 'left').merge(
                                third_dialed_time[id_tat_lst + ['third_dialed_tat']], on = id_tat_lst, how = 'left' )
        
        return fst_dialed_tat

    fst_dialed_tat = dialed_tat(outbound_request_final_df)

    # FIRST_SECOND_THIRD_ATTEMPTED_TAT

    def attempted_tat(outbound_request_final_df):
        first_attempted_time = outbound_request_final_df[(outbound_request_final_df.call_rank==1) & (outbound_request_final_df.flag == 1) & (outbound_request_final_df.attempted == 1)].groupby(id_tat_lst
                                                    , as_index = False).agg(
                                {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'first_attempted_time'})

        first_attempted_time['first_attempted_tat'] = first_attempted_time.first_attempted_time - first_attempted_time.epoch_requested_time


        second_attempted_time = outbound_request_final_df[(outbound_request_final_df.call_rank==2) & (outbound_request_final_df.flag == 1) & (outbound_request_final_df.attempted == 1)].groupby(id_tat_lst
                                                    , as_index = False).agg(
                                {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'second_attempted_time'})

        second_attempted_time['second_attempted_tat'] = second_attempted_time.second_attempted_time - second_attempted_time.epoch_requested_time


        third_attempted_time = outbound_request_final_df[(outbound_request_final_df.call_rank==3) & (outbound_request_final_df.flag == 1) & (outbound_request_final_df.attempted == 1)].groupby(id_tat_lst
                                                , as_index = False).agg(
                                {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'third_attempted_time'})

        third_attempted_time['third_attempted_tat'] = third_attempted_time.third_attempted_time - third_attempted_time.epoch_requested_time


        fst_attempted_tat = first_attempted_time[id_tat_lst + ['first_attempted_tat']].merge(
                                second_attempted_time[id_tat_lst + ['second_attempted_tat']], on = id_tat_lst, how= 'left').merge(
                                    third_attempted_time[id_tat_lst + ['third_attempted_tat']], on = id_tat_lst, how= 'left' )
        return fst_attempted_tat

    fst_attempted_tat = attempted_tat(outbound_request_final_df)

    # FIRST_SECOND_THIRD_CONNECTED_TAT

    def connected_tat(outbound_request_final_df):
        first_connected_time = outbound_request_final_df[(outbound_request_final_df.call_rank==1) & (outbound_request_final_df.flag == 1) & (outbound_request_final_df.system_disposition == 'CONNECTED')].groupby(id_tat_lst
                                                , as_index = False).agg(
                                {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'first_connected_time'})

        first_connected_time['first_connected_tat'] = first_connected_time.first_connected_time - first_connected_time.epoch_requested_time


        second_connected_time = outbound_request_final_df[(outbound_request_final_df.call_rank==2) & (outbound_request_final_df.flag == 1) & (outbound_request_final_df.system_disposition == 'CONNECTED')].groupby(id_tat_lst
                                                    , as_index = False).agg(
                                {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'second_connected_time'})

        second_connected_time['second_connected_tat'] = second_connected_time.second_connected_time - second_connected_time.epoch_requested_time


        third_connected_time = outbound_request_final_df[(outbound_request_final_df.call_rank==3) & (outbound_request_final_df.flag == 1) & (outbound_request_final_df.system_disposition == 'CONNECTED')].groupby(id_tat_lst
                                                , as_index = False).agg(
                                {'epoch_requested_time':'min', 'dialed_epoch_ts':'min'}).rename(columns ={'dialed_epoch_ts':'third_connected_time'})

        third_connected_time['third_connected_tat'] = third_connected_time.third_connected_time - third_connected_time.epoch_requested_time


        fst_connected_tat = first_connected_time[id_tat_lst + ['first_connected_tat']].merge(
                                second_connected_time[id_tat_lst + ['second_connected_tat']], on = id_tat_lst, how= 'left').merge(
                                    third_connected_time[id_tat_lst + ['third_connected_tat']], on = id_tat_lst, how= 'left' )
        return fst_connected_tat
    
    fst_connected_tat = connected_tat(outbound_request_final_df)


    
    # merging fst_dialed, fst_attempted, fst_connected tat as fst_tat


    fst_tat = fst_dialed_tat.merge(
            fst_attempted_tat, on = id_tat_lst, how = 'left').merge(
            fst_connected_tat, on = id_tat_lst, how = 'left')

    fst_tat['date'] = fst_tat.date.dt.date

    return fst_tat


# Attempts of calls getting connected

def attempts_connected(outbound_request_final_df):

    # Defining List
    date_tat_lst = ['date', 'hour', 'city', 'ivr', 'vicinity', 'call_type_modified', 'order_status', 'vehicle_name','call_raisedby', 'vehicle_id']

    # Creating Percentile Distribution for Attempts Of Connected Calls

    
    orfd_connected = outbound_request_final_df[outbound_request_final_df.system_disposition == 'CONNECTED']

    Attempts_connected_tat = pd.DataFrame()
    result = []

    bins = [.1, .2, .3, .4, .5, .6, .7, .8, .9, .95, .99]

    for x in bins:

        df_temp1 = orfd_connected.groupby(date_tat_lst, as_index=False)[['connected_attempts']].quantile(x)
        da_num = len(df_temp1)

        Attempts_connected_tat = pd.concat([Attempts_connected_tat, df_temp1])

        result.extend(repeat(x,da_num))
        
    Attempts_connected_tat.reset_index(inplace=True, drop=True)
    del df_temp1
    Attempts_connected_tat['percentile'] = result
    Attempts_connected_tat.set_index('percentile', drop=True, inplace=True)



    Attempts_connected_tat.rename(index={0.10: 'percentil_10', 0.20: 'percentil_20', 0.30: 'percentil_30', 
                            0.40: 'percentil_40', 0.50: 'percentil_50', 0.60: 'percentil_60', 
                            0.70: 'percentil_70', 0.80: 'percentil_80', 0.90: 'percentil_90', 
                            0.95: 'percentil_95', 0.99: 'percentile_99'}, inplace =True)

    Attempts_connected_tat.reset_index(inplace = True)
    Attempts_connected_tat.rename(columns = {'level_1': 'percentile'}, inplace = True)
    Attempts_connected_tat =  Attempts_connected_tat[date_tat_lst + ['percentile','connected_attempts']]


    Attempts_connected_tat.sort_values(by = ['date', 'city', 'connected_attempts'], inplace= True, ignore_index= True)

    Attempts_connected_tat['date'] = Attempts_connected_tat.date.dt.date
    Attempts_connected_tat = Attempts_connected_tat.astype({'connected_attempts':'int'})



    return Attempts_connected_tat


# Writing Data to Redshift

def write_data(lst):

    for i in range(len(lst)):
   
        j = 0
        
        tbl = lst[i][j+1]   
        df = lst[i][j]
        
        utils.write_to_redshift(tbl, df)
    




if __name__ == '__main__':

    stage = """
    #----------------------Fetching Data-------------------
    """
    print(stage)
    
    try:
        outbound_request, mapping_for_outbound_to_request, outbound_calls_df, city_vicinity_df = fetch_data()
        print("Fetching Data ran succesfully....\n")
        
    except Exception as e:
        print("ERRRROOOORRR: ",e)

    
    stage = """
    #------------------Transforming Data-------------------
    """
    print(stage)
    
    try:
        outbound_request_req_col_df, outbound_calls_require_cols_df, outbound_request,  city_vicinity_req_cols_df =  transforming_data(outbound_request, outbound_calls_df, city_vicinity_df)
        print("Transforming Data ran succesfully....\n")

    except Exception as e:
        print("ERRRROOOORRR: ",e)

    

    
    stage = """
    #--------------------Merging Data----------------------
    """
    print(stage)

    try:
        outbound_request_final_df = merging(outbound_request_req_col_df, mapping_for_outbound_to_request,  outbound_calls_require_cols_df, city_vicinity_req_cols_df)
        print("Merging Data ran succesfully....\n")

    except Exception as e:
        print("ERRRROOOORRR: ",e)
    
    


    stage = """
    #--------------Generating Overall Summary--------------
    """
    print(stage)
    
    try:
        summary   =  summarydf(outbound_request_final_df)
        print("Summary Data ran succesfully....\n")

    except Exception as e:
        print("ERRRROOOORRR: ",e)

    


    stage = """
    #---------------Generating Tat Summary-----------------
    """
    print(stage)
    
    try:
        fst_tat  =  tatdf(outbound_request_final_df)
        print("Tat Data ran succesfully....\n")

    except Exception as e:
        print("ERRRROOOORRR: ",e)

    


    stage = """
    #---------Generating Attempts_Connected Summary--------
    """
    print(stage)

    try:
        Attempts_connected_tat = attempts_connected(outbound_request_final_df)
        print("Attempts_connected_tat Data ran succesfully....\n")

    except Exception as e:
        print("ERRRROOOORRR: ",e)

    
    stage = """
    #------------Writing DataFrames to Redshift------------
    """
    print(stage)

    lst = [[summary , 'mj_test_outbound_summary'], [fst_tat , 'mj_test_fst_tat'], [ Attempts_connected_tat , 'mj_test_attempts_connect']]
    
    try:
        write_data(lst)
        print()
        print("Date Dumped into Redshift succesfully....\n")

    except Exception as e:
        print("ERRRROOOORRR: ",e)
    



print(outbound_request.head(), end='\n\n')
print(mapping_for_outbound_to_request.head(), end='\n\n')
print(outbound_calls_df.head(), end='\n\n')
print(city_vicinity_df.head(), end='\n\n')
print(outbound_request_final_df.head(), end= '\n\n')
print(summary, end='\n\n')
print(fst_tat.head(), end='\n\n')
print(Attempts_connected_tat.head(), end='\n\n')






