##-----------Imports-------------

import warnings
warnings.filterwarnings("ignore")
import pandas as pd
pd.options.display.max_columns=None
import traceback
import sched
import time
from datetime import datetime, timedelta,timezone
import os
import pytz
import time
phoenix_tz = pytz.timezone('America/Phoenix')

from blb_functions import build_sql_query_from_tree, get_op_relative_directory, logger,check_phone_num_length,get_smart_suite_s3_client,get_s3_client,dep_environment,APP_VERSION,DEBUG,query_db,logging_function,upload_text_tos3

scheduler = sched.scheduler(time.time, time.sleep)
##-----Function definitions------

#print('sched app')

def generate_dependency_files():
    #logger.warning('Dependecy CSV generator is triggered')
    #logging_function(log_level='INFO',log_message='Dependecy CSV generator is triggered')
    #print('other func called')
    bucket =  "mobivity-datascience"
    ##--------user_inputs-----------
    #print_query = False
    key = ''

    file_directory = 'dependencies'
    
    fname = 'company_names.csv'
    query = """select distinct companyname company_names, companyid comp_id  from dw.dim_company dc 
    where dc.active =1
    order by companyname asc """
    df = query_db(query,bucket, key, print_info=False)
    df.to_csv(f'{file_directory}/{fname}')
    #print(fname, 'completed')

    fname = 'state_names.csv'
    query = "select distinct state_name  from dw.dim_zipcode dz order by state_name asc "
    df = query_db(query,bucket, key, print_info=False)
    df.to_csv(f'{file_directory}/{fname}')
    #print(fname, 'completed')

    fname = 'app_titles.csv'
    query = "select distinct app_title  from dw.dim_connected_rewards dcr order by app_title asc"
    df = query_db(query,bucket, key, print_info=False)
    df.to_csv(f'{file_directory}/{fname}')
    #print(fname, 'completed')

    fname = 'opt_in_source.csv'
    query = """select distinct dc.companyid company_id, ds.opt_in_source
            from dw.dim_user du 
            join dw.dim_subscription ds on ds.user_id = du.user_id 
            join dw.dim_campaign dc on dc.campaign_id = ds.campaign_id 
            join dw.dim_company dc2 on dc2.companyid  = dc.companyid and du.company_id  = dc2.companyid 
            where ds.is_subscribed_flag =1 
            and ds.is_double_optin_flag =1
            and dc2.active =1
            and dc2.active = 1
            and dc.is_active_flag = 1
            and dc.is_campaign_hidden_flag = 0 
            order by ds.opt_in_source asc"""
    df = query_db(query,bucket, key, print_info=False)
    df.to_csv(f'{file_directory}/{fname}')
    #print(fname, 'completed')

    fname = 'bu_names.csv'
    query = """select  distinct company.companyid company_id, entity.name as bu_name from
            dw.dim_user users
            join dw.dim_subscription subs on subs.user_id = users.user_id 
            join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
            join dw.dim_company company on company.companyid = camps.companyid
            left join dw.dim_entity entity on entity.id = camps.entity_id
            where
            subs.is_subscribed_flag =1
            and subs.is_double_optin_flag =1
            and camps.is_active_flag =1
            and camps.is_campaign_hidden_flag = 0
            and users.phone is not null 
            and company.active =1
            order by bu_name asc
            """
    df = query_db(query,bucket, key, print_info=False)
    df.to_csv(f'{file_directory}/{fname}')
    #print(fname, 'completed')
    log_data = open('logs_blb_app.log').read()
    upload_text_tos3(log_data,bucket='mobivity-datascience',filename_key=f'flight_datascience/bcast_list_builder/logs/logs_blb_app_{dep_environment}.log')
    #logger.warning('log file uploaded successfully')
    #logging_function(log_level='WARNING',log_message=)
    #logger.warning('Starting to delete older OP files')
    op_file_list = os.listdir('output')
    now_str = datetime.now()
    now_str = now_str.astimezone(tz=phoenix_tz).date()
    now_str = now_str-timedelta(days=2)
    now_str = str(now_str)
    now_str=now_str.replace('-','')
    delete_list = [x for x in op_file_list if x.split('_')[-1].split('.zip')[0][:8] <= now_str]
    n_del_files = len(delete_list)
    #logger.warning(f'Starting to delete {n_del_files} output files <= {now_str}')
    for x in delete_list:
        os.remove(f'output/{x}')

def gen_dep_try_exc():
    try:
        generate_dependency_files()
        #logger.warning('Cron finished successfully')
        #logging_function(log_level='WARNING',log_message='Cron finished successfully')
    except Exception as e:
        error_str = traceback.format_exc()
        logger.error(error_str)
        #logging_function(log_level='ERROR',log_message=error_str)
        raise Exception(error_str)


def calculate_next_execution_6am():
    now = datetime.now()
    now = now.astimezone(tz=phoenix_tz)
    tomorrow = now + timedelta(hours=1)
    next_execution = datetime(tomorrow.year, tomorrow.month, tomorrow.day, tomorrow.hour, 0, 0,tzinfo = phoenix_tz)

    local_time_zone = datetime.now(timezone.utc).astimezone().tzinfo
    #logger.warning(next_execution)
    #logger.warning(local_time_zone)
    next_execution = next_execution.astimezone(tz=local_time_zone)
    next_execution = datetime(next_execution.year, next_execution.month, next_execution.day, next_execution.hour, next_execution.minute, 0)
    #logger.warning(next_execution)
    return time.mktime(next_execution.timetuple())

def schedule_task_6am():
    next_execution = calculate_next_execution_6am()
    scheduler.enterabs(next_execution,priority=2,action= schedule_task_6am)
    gen_dep_try_exc()  # Execute the function
    #logger.warning(f"Function at 6 AM scheduled for {datetime.fromtimestamp(next_execution)}")
    #logging_function(log_level='WARNING',log_message=f"Function at 6 AM scheduled for {datetime.fromtimestamp(next_execution)}")

schedule_task_6am()
logger.warning("Scheduler started")
#logging_function(log_level='WARNING',log_message="Scheduler started")
scheduler.run()
