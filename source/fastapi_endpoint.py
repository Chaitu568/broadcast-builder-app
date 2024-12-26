
from fastapi import FastAPI, File, UploadFile, Request
#from typing import Union
from fastapi.encoders import jsonable_encoder
import requests
import csv
import codecs
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import s3fs
import time
import io
import traceback
import os
from io import BytesIO
#sys.path.append(parent_directory)
#from functions import query_db
import redshift_connector
import json
import datetime
import logging
from blb_functions import build_sql_query_from_tree, get_op_relative_directory, logger,check_phone_num_length,get_smart_suite_s3_client,get_s3_client,dep_environment,APP_VERSION,DEBUG,query_db,upload_json_tos3, param_file, logging_function, function_for_st_query, s3_read_text_file, upload_csv_tos3


with open(param_file) as json_file:
    app_config = json.load(json_file)

log_filename = app_config['log_filename']
# LOG_FORMAT = '[%(levelname)s]: %(asctime)s %(name)s, %(funcName)s called. %(message)s'
# logging.basicConfig(filename=log_filename,level=logging.INFO, format=LOG_FORMAT)
# logger = logging.getLogger(name='fastapi_logger')
logger.warning('app started')
app = FastAPI()
s3_comm_on=True

PORT_NUM = app_config['backend_port']
backend_base_url =app_config['backend_url']
##uvicorn flask_api_endpoint:app --reload --port 8001


app.zipdf = pd.DataFrame()
app.phonedf = pd.DataFrame()
app.exclude_zipcode = pd.DataFrame()
app.include_zipcode = pd.DataFrame()
app.add_phone_list = pd.DataFrame()
app.remove_phone_list = pd.DataFrame()
app.intersect_phone_list = pd.DataFrame()
#app.state_manager = {}
vers = time.time_ns()
#app. = pd.DataFrame()



# def delete_versions(version):
#     app.state_manager.pop(version)



@app.post("/get_version/")
def get_version(version:str=""):
    #app.state_manager[version]={}
    print(version)

@app.post("/share_field_dict/{shared_field_dict}")
def share_field_dict(shared_field_dict:str, version: int=0):
    shared_field_dict = json.loads(shared_field_dict)

@app.get('/get_file/')
def send_file():
    df = pd.read_csv('dependencies/state_names.csv')
    df = pd.DataFrame()
    stringio  = io.StringIO()
    df.to_csv(stringio, index=False,header=False)
    #resp = requests.post(url=f'http://localhost:{PORT_NUM}/filter_include_zipcode/?version={VERSION}', files={'file':stringio.getvalue()}) 
    resp = {'files':stringio.getvalue(), 'author':'Hari'}
    resp = json.dumps(resp )
    return resp


@app.post('/log')
async def log_info(request: Request):
    try:
        data = await request.json()
        log_info = data['log_info']
        log_level = data['log_level']
        if log_level=='ERROR':
            logger.error(log_info)
        elif log_level=='WARNING':
            logger.warning(log_info)
        else :
            logger.info(log_info)

    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise(e)
        


@app.post('/st_query/')
async def get_st_query(request: Request):
    try:
        logger.info('start')
        resp = {}
        data = await request.json()
        resp  = function_for_st_query(data)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        print(error_str)
        resp['phone_count'] = None
        resp['op_file_names'] = None
        resp['fin_query'] = 'Error Occured '+str(error_str)
    resp = json.dumps(resp)
    logger.info('end')
    return resp

@app.get('/get_old_query/{version}')
def get_old_query(compid: int=0,query_tag: str | None = None,version: int=0):
    try:
        bucket = app_config['bucket']
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        key = f"{op_directory}/query_all_params_{version}.json"
        old_query_json = s3_read_text_file(bucket, key)
        old_query_json = json.loads(old_query_json)
        old_tree = old_query_json['tree_config']
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        return {"message": f"{error_str}"}
    return old_query_json



@app.post("/filter_include_zipcode")
def upload1(file: UploadFile = File(...), compid: int=0,query_tag: str | None = None,version: int=0):
    try:
        df = pd.read_csv(file.file)  
        bucket =  app_config['bucket']
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        filename_key = f'{op_directory}/filter_include_zipcode.csv'
        upload_csv_tos3(df, bucket, filename_key)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        return {"message": f"{error_str}"}
    finally:
        file.file.close()

@app.post("/filter_exclude_zipcode")
def upload2(file: UploadFile = File(...), compid: int=0,query_tag: str | None = None,version: int=0):
    try:
        df = pd.read_csv(file.file)
        bucket =  app_config['bucket']
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        filename_key = f'{op_directory}/filter_exclude_zipcode.csv'
        upload_csv_tos3(df, bucket, filename_key)
        print(version)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        return {"message": f"{error_str}"}
    finally:
        file.file.close()
    return {"message": f"Successfully uploaded {file.filename}"}

@app.post("/filter_add_phone_list/")
async def upload3(file: UploadFile = File(...), compid: int=0,query_tag: str | None = None,version: int=0):
    compid = int(compid)
    try:
        df = pd.read_csv(file.file)
        bucket =  app_config['bucket']
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        filename_key = f'{op_directory}/filter_add_phone_list.csv'
        upload_csv_tos3(df, bucket, filename_key)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        return {"message": f"{error_str}"}
    finally:
        file.file.close()
    return {"message": f"Successfully uploaded {file.filename}"}

@app.post("/filter_remove_phone_list")
def upload4(file: UploadFile = File(...), compid: int=0,query_tag: str | None = None,version: int=0):
    try:
        df = pd.read_csv(file.file)
        bucket =  app_config['bucket']
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        filename_key = f'{op_directory}/filter_remove_phone_list.csv'
        upload_csv_tos3(df, bucket, filename_key)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        return {"message": f"{error_str}"}

    finally:
        file.file.close()
    return {"message": f"Successfully uploaded {file.filename}"}

@app.post("/filter_add_test_phone_list")
def upload4(file: UploadFile = File(...), compid: int=0,query_tag: str | None = None,version: int=0):
    try:
        df = pd.read_csv(file.file)
        bucket =  app_config['bucket']
        #print(vers)
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        filename_key = f'{op_directory}/filter_add_test_phone_list.csv'

        upload_csv_tos3(df, bucket, filename_key)
        print(version)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        return {"message": f"{error_str}"}
    finally:
        file.file.close()
    return {"message": f"Successfully uploaded {file.filename}"}

@app.post("/filter_intersect_phone_list")
def upload5(file: UploadFile = File(...), compid: int=0,query_tag: str | None = None,version: int=0):
    try:
        df = pd.read_csv(file.file)
        bucket =  app_config['bucket']
        #print(vers)
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        filename_key = f'{op_directory}/filter_intersect_phone_list.csv'
        upload_csv_tos3(df, bucket, filename_key)
        print(version)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        return {"message": f"{error_str}"}
    finally:
        file.file.close()
    return {"message": f"Successfully uploaded {file.filename}"}

@app.get('/hi/')
def say_hi():
    return 'hi'

@app.get('/rerun_old_query/')
async def get_old_query(compid: int=0,blb_ref_num: str | None = None):
    try:
        version = blb_ref_num.split('_')[-1]
        #print(blb_ref_num[:-1])
        query_tag = '_'.join(blb_ref_num.split('_')[:-1])
        bucket = app_config['bucket']
        print(version)
        print(query_tag)
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        key = f"{op_directory}/query_all_params_{version}.json"
        old_query_json = s3_read_text_file(bucket, key)
        old_query_json = json.loads(old_query_json)
        resp  = function_for_st_query(old_query_json)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        #raise(e)
        return {"message": f"{error_str}"}
    return resp['phone_count']