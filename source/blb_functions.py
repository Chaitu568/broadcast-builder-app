APP_VERSION  = 'v1.17.4'
DEBUG = False
#dep_environment = 'local'#'qa' #'local'#'prod'


import boto3, traceback
import logging
from logging.handlers import TimedRotatingFileHandler
import io
import pandas as pd
import requests
import json
import os
from difflib import get_close_matches
from botocore.exceptions import ClientError
import datetime
import redshift_connector

new_model_compid_list = [1207]
tree_rule_operator_reverse_map = {   
                                    '=':'equal',
                                    '!=':'select_not_equals',
                                    '>':'greater',
                                    '>=':'greater_or_equal',
                                    '<':'less',
                                    '<=':'less_or_equal',
                                }

tree_rule_operator_map = {
    'equal':'=',
    'select_equals':'=',
    'select_not_equals':'!=',
    'not_equal':'!=',
    'select_any_in':'IN',
    'select_not_any_in':'NOT IN',
    'greater_or_equal':'>=',
    'less':'<',
    'greater':'>',
    'less_or_equal':'<=',
    'between':'BETWEEN',
    'not_between':'NOT BETWEEN',
    'like':'ILIKE',
    'ends_with':'ILIKE',
    'starts_with':'ILIKE',
    'not_like':'NOT ILIKE',
    'is_empty':'=',
    'is_not_empty':'!='
}
with open('env_file.json') as json_file:
    env_file = json.load(json_file)
dep_environment = env_file['env']
#dep_environment = os.environ['environment']
print('found deploy environment as ', dep_environment)

if dep_environment == 'local':
    param_file='config_blb_app_local.json'
else:
    param_file='config_blb_app.json'

s3_comm_on=True
with open(param_file) as json_file:
    app_config = json.load(json_file)
log_filename = app_config['log_filename']
LOG_FORMAT = '[%(asctime)s] %(funcName)s called. %(message)s'
# log_formatter = logging.Formatter(LOG_FORMAT)
logging.basicConfig(filename=log_filename,level=logging.WARNING, format=LOG_FORMAT)
# log_handler = TimedRotatingFileHandler(log_filename, when='M', interval=1)
# log_handler.setFormatter(log_formatter)
# log_handler.setLevel(logging.INFO)
logger = logging.getLogger(name='blb_logger')
#logger.warning('function file loaded')
# logger.addHandler(log_handler)

negative_operators = {'!=':'=','NOT IN':'IN','NOT BETWEEN':'BETWEEN','NOT ILIKE':'ILIKE'}

PORT_NUM = app_config['backend_port']
backend_base_url =app_config['backend_url']

def logging_function(log_level='',log_message=''):
  """for error set log_level = ERROR"""
  #log_level = 'WARNING'
  log_data = {'log_level':log_level,'log_info':log_message}
  url_reposnse = requests.post(url = f'{backend_base_url}:{PORT_NUM}/log', json=log_data)


def sql_dtype_correction(field,operator,value):
    new_operator = operator_correction(operator)
    
    if field in [
         'device_os',
         'installed_app',
         'seen_app',
         'broadcast_date',
         'last_seen_date',
         'clicked_app',
         'state_name',
         'bu_name',
         'opt_in_source',
         'camp_keyword',
         'redeemer_flag',
         'redeemer_time_of_day',
         'redeemer_day_of_week','promo_name_clicked','installed_with_site_id',
         'engaged'] and (not (new_operator.endswith('IN') or new_operator.endswith('LIKE'))):
        
        return f"'{value}'"
    elif operator in ['like','not_like'] or ('promo_tag_clicked' in field)  :
        return f"'%{value}%'"
    elif operator in ['ends_with'] :
        return f"'%{value}'"
    elif operator in ['starts_with'] :
        return f"'{value}%'"
    else:
        return value

def field_condition_count_func(field,field_condition_counter):
    if field in list(field_condition_counter.keys()):
        field_condition_counter[field]+=1
    else:
        field_condition_counter[field]=1
    return field+'_'+str(field_condition_counter[field])

def get_sql_condition(field,new_field_name,new_operator, new_value):
    #if 'install_likelihood' in new_field_name:
    #    sql_condition = f"{new_field_name} {new_operator} 'install_likelihood'"
    #else:
    sql_condition = f"{new_field_name} {new_operator} {new_value}"
    return sql_condition


def operator_correction(operator):
    return tree_rule_operator_map[operator]

def reverse_neg_operator(operator,negative_operators):
    if operator in list(negative_operators.keys()):
        operator = negative_operators[operator]
    return operator

def sql_modules(field,new_field_name,operator, value, compid,inner_filter_fields,negative_operators):
    logger.info('start')
    #st = ''
    
    operator = reverse_neg_operator(operator,negative_operators)
    if field == 'device_os':
        subq = f"""left join 
        /* START sql for Device_OS   */
        (select distinct users.phone dt_phone , device_os as {new_field_name} from
                dw.dim_user users
                join dw.dim_subscription subs on subs.user_id = users.user_id 
                join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
                join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid
                join (select * from (select phone, device_os, last_date_device from 
                (select row_number() over(partition by phone order by last_date_device) as latest, phone, device_os, last_date_device from dw.dim_device)a 
                where latest=1 )b) devices
                on devices.phone = users.phone
                and users.company_id in ({compid})
                and subs.is_subscribed_flag =1
                and subs.is_double_optin_flag =1
                and camps.is_active_flag =1
                and camps.is_campaign_hidden_flag = 0
                and users.phone is not null) {new_field_name}_table
                /* END sql for Device_OS  */  
                on {new_field_name}_table.dt_phone = users.phone"""
        return subq,inner_filter_fields

    if  field == 'installed_app':
        subq = f"""left join 
        /* START sql for Installed APP  */
        (select distinct du.phone ia_phone, 'installed_app' as {new_field_name}
        from dw.fact_game_event fgi
        join dw.dim_user du on fgi.user_id = du.user_id
        join dw.dim_connected_rewards cr on cr.cr_id = fgi.cr_id
        where fgi.event_name = 'install'
        and app_title {operator} {value} ) {new_field_name}_table
        /* END sql for Installed APP  */
        on {new_field_name}_table.ia_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields

    if field == 'clicked_app' :
        subq = f"""left join 
        /* START sql for Clicked on APP */
        (select distinct du.phone ca_phone, 'clicked_app' as {new_field_name}
        from dw.fact_sms_event fse
        join dw.dim_user du on fse.user_id = du.user_id
        join dw.dim_connected_rewards cr on cr.install_promo_id = fse.ssms_config_id
        where fse.event_desc ilike '%click'
        and du.company_id in ({compid}) 
        and app_title {operator} {value}  ){new_field_name}_table
        /* END sql for Clicked on APP  */ 
        on {new_field_name}_table.ca_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields
    if field == 'clicked_promo_by_category' :
        subq = f"""left join 
        /* START sql for Clicked on Promo By Category / Tag */
        (select distinct du.phone cp_bc_phone, 'clicked_promo_by_category' as {new_field_name}
        from dw.fact_sms_event fse
        join dw.dim_user du on fse.user_id = du.user_id
        join dw.dim_promo_config dpc on dpc.promo_id  = fse.ssms_config_id
        where fse.event_desc ilike '%click'
        and du.company_id in ({compid}) 
        and dpc.categories {operator} {value}  ){new_field_name}_table
        /* END sql for Clicked on Promo By Category / Tag */ 
        on {new_field_name}_table.cp_bc_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields

    if field ==  'state_name':
        subq = f"""left join 
        /* START sql for State Name   */
        (select distinct users.phone state_tab_phone, nvl(dz.state_name,'unknown') {new_field_name}, users.zip as zipcode from
        dw.dim_user users
        join dw.dim_subscription subs on subs.user_id = users.user_id 
        join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
        join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid
        left join dw.dim_zipcode dz on dz.zip = users.zip
        where company.companyid in ({compid})
        and subs.is_subscribed_flag =1
        and subs.is_double_optin_flag =1
        and camps.is_active_flag =1
        and camps.is_campaign_hidden_flag = 0
        and users.phone is not null) {new_field_name}_table
        /* END sql for State Name */
        on {new_field_name}_table.state_tab_phone = users.phone"""
        return subq,inner_filter_fields

    if field == 'bu_name':
        subq = f"""left join
            /* START sql for BU Names   */
            (select  distinct users.phone bu_tab_phone, 'bu_name' as {new_field_name} from
            dw.dim_user users
            join dw.dim_subscription subs on subs.user_id = users.user_id 
            join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
            join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid
            left join dw.dim_entity entity on entity.id = camps.entity_id
            where
            subs.is_subscribed_flag =1
            and subs.is_double_optin_flag =1
            and camps.is_active_flag =1
            and camps.is_campaign_hidden_flag = 0
            and users.phone is not null
            and company.companyid in ({compid})
            and entity.name {operator} {value}) {new_field_name}_table
            /* END sql for BU Names */
            on {new_field_name}_table.bu_tab_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields
    
    if field == 'install_probability':
        subq = f"""left join 
        /* START sql for Install Probability   */
        (select distinct u.phone install_proba_phone, install_probability as {new_field_name} from
        dw_aggr.model_broadcast mb
        join dw.dim_user u on mb.user_id=u.user_id
        where 
        u.company_id in ({compid}) ){new_field_name}_table
        /* END sql for Install Probability */
        on {new_field_name}_table.install_proba_phone = users.phone"""
        return subq,inner_filter_fields

    if field == 'install_likelihood':
        if compid in new_model_compid_list:
            subq = f"""left join 
            /* START sql for Install Likelihoood   */
            (select distinct u.phone ins_likl_phone, true as {new_field_name} from
            dw_aggr.model_broadcast mb
            join dw.dim_user u on mb.user_id=u.user_id
            where 
            u.company_id in ({compid})
            and install_probability >= 0.5){new_field_name}_table
            /* END sql for Install Likelihoood */
            on {new_field_name}_table.ins_likl_phone = users.phone"""
        else:
            subq = f"""left join 
            /* START sql for Install Likelihoood   */
            (select distinct u.phone ins_likl_phone, true as {new_field_name} from
            dw_aggr.model_broadcast mb
            join dw.dim_user u on mb.user_id=u.user_id
            where 
            u.company_id in ({compid})
            and install_likelihood in (3,4)){new_field_name}_table
            /* END sql for Install Likelihoood   */
            on {new_field_name}_table.ins_likl_phone = users.phone"""
        #inner_filter_fields.append(new_field_name)

        return subq,inner_filter_fields
    if field == 'redeemer_flag':
        subq = f"""left join
                /* START sql for Redeemer, Non-Redeemer  */
                (select  distinct users.phone mb_redm_phone,
                case when min(days_since_last_redemption)>=0 then 'Redeemer' else 'Non-Redeemer' end {new_field_name}
                from
                dw.dim_user users
                join dw.dim_subscription subs on subs.user_id = users.user_id 
                join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
                join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid 
                join dw_aggr.model_broadcast mb on mb.user_id = users.user_id and subs.subscription_id = mb.subscription_id
                where
                subs.is_subscribed_flag =1
                and subs.is_double_optin_flag =1
                and camps.is_active_flag =1
                and camps.is_campaign_hidden_flag = 0
                and users.phone is not null
                and company.companyid in ({compid})
                and (days_since_last_redemption >=0 or days_since_last_redemption is null)
                group by users.phone ) {new_field_name}_table
                /* END sql for Redeemer, Non-Redeemer */
                on {new_field_name}_table.mb_redm_phone = users.phone"""
        return subq,inner_filter_fields

    if field == 'redeemer_time_of_day':
        subq = f"""left join
                /* START sql for Redeem Time of the Day */
                (select  distinct du.phone rtod_phone, 'redeemer_time_of_day' as {new_field_name}
                from dw.dim_user du
                join dw.dim_subscription ds on ds.user_id = du.user_id
                join dw.dim_campaign dc on dc.campaign_id = ds.campaign_id
                join dw.dim_company dc2 on dc2.companyid  = dc.companyid and du.company_id  = dc2.companyid
                join dw.fact_sms_event fse on du.user_id=fse.user_id
                join dw_aggr.model_broadcast mb on du.user_id=mb.user_id and ds.subscription_id = mb.subscription_id
                where ds.is_subscribed_flag =1
                and ds.is_double_optin_flag =1
                and dc2.active = 1
                and dc.is_active_flag = 1
                and dc.is_campaign_hidden_flag = 0
                and dc.companyid ={compid}
                and fse.event_desc ilike '%redeem'
                and mb.redeem_day_part {operator} {value}) {new_field_name}_table
                /* END sql for Redeem Time of the Day */
                on {new_field_name}_table.rtod_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields
    if field == 'redeemer_day_of_week':
        subq = f"""left join
                /* START sql for Redeem Day of the Week */
                (select  distinct du.phone rdow_phone, 'redeemer_day_of_week' as {new_field_name}
                from dw.dim_user du
                join dw.dim_subscription ds on ds.user_id = du.user_id
                join dw.dim_campaign dc on dc.campaign_id = ds.campaign_id
                join dw.dim_company dc2 on dc2.companyid  = dc.companyid and du.company_id  = dc2.companyid
                join dw.fact_sms_event fse on du.user_id=fse.user_id
                where ds.is_subscribed_flag =1
                and ds.is_double_optin_flag =1
                and dc2.active = 1
                and dc.is_active_flag = 1
                and dc.is_campaign_hidden_flag = 0
                and dc.companyid ={compid}
                and fse.event_desc ilike '%redeem'
                and TO_CHAR(fse.event_ts, 'Day') {operator} {value}) {new_field_name}_table
                /* END sql for Redeem Day of the Week */
                on {new_field_name}_table.rdow_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields
    if field == 'days_since_last_redemption':
        subq = f"""left join
                /* START sql for Days since last redemption */
                (select  distinct users.phone mb_redm_phone, 
                min(days_since_last_redemption) {new_field_name}
                from
                dw.dim_user users
                join dw.dim_subscription subs on subs.user_id = users.user_id 
                join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
                join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid 
                join dw_aggr.model_broadcast mb on subs.subscription_id = mb.subscription_id
                where
                subs.is_subscribed_flag =1
                and subs.is_double_optin_flag =1
                and camps.is_active_flag =1
                and camps.is_campaign_hidden_flag = 0
                and users.phone is not null
                and company.companyid in ({compid})
                and (days_since_last_redemption >=0 or days_since_last_redemption is null)
                group by users.phone ) {new_field_name}_table
                /* END sql for Days since last redemption */
                on {new_field_name}_table.mb_redm_phone = users.phone"""
        return subq,inner_filter_fields
    if field == 'tenure':
        subq = f"""left join
                /* START sql for Tenure */
                (select  distinct users.phone mb_tr_phone,
                max(tenure_days) {new_field_name}
                from
                dw.dim_user users
                join dw.dim_subscription subs on subs.user_id = users.user_id 
                join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
                join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid 
                join dw_aggr.model_broadcast mb on mb.user_id = users.user_id and subs.subscription_id = mb.subscription_id
                where
                subs.is_subscribed_flag =1
                and subs.is_double_optin_flag =1
                and camps.is_active_flag =1
                and camps.is_campaign_hidden_flag = 0
                and users.phone is not null
                and company.companyid in ({compid})
                group by users.phone ) {new_field_name}_table
                /* END sql for Tenure */
                on {new_field_name}_table.mb_tr_phone = users.phone"""
        return  subq,inner_filter_fields
    if field ==  'engaged':   
        subq = f"""left join 
        /* START sql for Engaged , Not Engaged */
        (select distinct se.user_id engage_user_id, 'Engaged' as {new_field_name} from dw_aggr.ssms_event se 
        join dw.dim_user du on du.user_id  = se.user_id
        where first_click_action_datetime is not null
        and du.company_id in ({compid}) ) {new_field_name}_table
        /* END sql for Engaged , Not Engaged */
        on {new_field_name}_table.engage_user_id = users.user_id"""
        return  subq,inner_filter_fields
    if field ==  'camp_keyword':
        subq = f"""left join (
        /* START sql for Campaign Keyword */
        select distinct users.phone camp_phone , 'camp_keyword' {new_field_name} from
        dw.dim_user users
        join dw.dim_subscription subs on subs.user_id = users.user_id 
        join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
        join dw.dim_company company on company.companyid = camps.companyid
        and subs.is_subscribed_flag =1
        and subs.is_double_optin_flag =1
        and camps.is_active_flag =1
        and camps.is_campaign_hidden_flag = 0
        and users.phone is not null
        where company.companyid in ({compid}) and  ( camps.keyword {operator} {value}  )){new_field_name}_table 
        /* END sql for Campaign Keyword */
        on {new_field_name}_table.camp_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields
    if field ==  'opt_in_source':
        subq = f"""left join (
        /* START sql for Campaign Keyword */
        select distinct users.phone camp_phone , 'opt_in_source' {new_field_name} from
        dw.dim_user users
        join dw.dim_subscription subs on subs.user_id = users.user_id 
        join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
        join dw.dim_company company on company.companyid = camps.companyid
        and subs.is_subscribed_flag =1
        and subs.is_double_optin_flag =1
        and camps.is_active_flag =1
        and camps.is_campaign_hidden_flag = 0
        and users.phone is not null
        where company.companyid in ({compid}) and  ( subs.opt_in_source {operator} {value}  )){new_field_name}_table 
        /* END sql for Campaign Keyword */
        on {new_field_name}_table.camp_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields
    if field ==  'promo_tag_clicked':
        subq = f"""left join (
        /* START sql for Campaign Keyword */
        select distinct users.phone promo_tag_click_phone, 'promo_tag_clicked' {new_field_name} from dw.fact_sms_event fse 
        join dw.dim_promo_config dpc on dpc.promo_id  = fse.ssms_config_id
        join (select promo_id promo_id_maxts, max(action_ts) max_ts from dw.dim_promo_config dpc group by promo_id ) dpc_max_ts
        on dpc_max_ts.promo_id_maxts = dpc.promo_id and dpc.action_ts = dpc_max_ts.max_ts
        join dw.dim_user users on users.user_id = fse.user_id 
        join dw.dim_subscription subs on subs.user_id = users.user_id 
        join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
        join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid
        where fse.event_desc ilike '%click'
        and subs.is_subscribed_flag =1
        and subs.is_double_optin_flag =1
        and camps.is_active_flag =1
        and camps.is_campaign_hidden_flag = 0
        and users.phone is not null
        and company.companyid in ({compid}) and  ( dpc.categories ilike {value}  )){new_field_name}_table 
        /* END sql for Campaign Keyword */
        on {new_field_name}_table.promo_tag_click_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields
    if field ==  'promo_name_clicked':
        subq = f"""left join (
        /* START sql for Campaign Keyword */
        select distinct users.phone promo_tag_click_phone, 'promo_name_clicked' {new_field_name} from dw.fact_sms_event fse 
        join dw.dim_user users on users.user_id = fse.user_id 
        join dw.dim_subscription subs on subs.user_id = users.user_id 
        join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
        join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid
        where fse.event_desc ilike '%click'
        and subs.is_subscribed_flag =1
        and subs.is_double_optin_flag =1
        and camps.is_active_flag =1
        and camps.is_campaign_hidden_flag = 0
        and users.phone is not null
        and company.companyid in ({compid}) and  ( lower(fse.ssms_config_id) {operator} lower({value})  )){new_field_name}_table 
        /* END sql for Campaign Keyword */
        on {new_field_name}_table.promo_tag_click_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields
    if field ==  'installed_with_site_id':
        subq = f"""left join (
        /* START sql for installed from Promo Id */
        select distinct users.phone ifp_phone, 'installed_with_site_id' {new_field_name} from dw.fact_game_event fge 
        join dw.dim_user users on users.user_id = fge.user_id 
        where fge.event_name = 'install'
        and users.company_id in ({compid}) and  ( lower(fge.site_id) {operator} lower({value})  )){new_field_name}_table 
        /* END sql for installed from Promo Id */
        on {new_field_name}_table.ifp_phone = users.phone"""
        inner_filter_fields.append(new_field_name)
        return subq,inner_filter_fields

def sql_for_seen_app_and_dates(selection_dict,op_cond, compid,inner_filter_fields):
    #logging_function(log_level='INFO',log_message=f'start: sql_for_seen_app_and_dates')
    
    st = list(selection_dict.keys())
    i = 1
    seen_app_i = 'seen_app_'+str(i)
    last_seen_date_i = 'last_seen_date_'+str(i)
    broadcast_date_i = 'broadcast_date_'+str(i)
    while (seen_app_i in st) or (last_seen_date_i in st) or (broadcast_date_i in st) :
        if (last_seen_date_i in st):
            
            
            
            if (last_seen_date_i in st):
                sel_lsd = f", max(date(db.broadcast_send_date)) as last_seen_date_{i}"
                gb_lsd_ad = f" group by sa_phone,seen_app_{i}_bcast "
                gb_lsd_d = f" group by sa_phone "
            else:
                sel_lsd=""
                gb_lsd_d=""
                gb_lsd_ad = ""
            if (seen_app_i in st):
                seen_app_name = selection_dict[seen_app_i]['value']
                seen_app_operator = selection_dict[seen_app_i]['new_operator']
                seen_app_operator = reverse_neg_operator(seen_app_operator, negative_operators)
                sel_sa = f", '{seen_app_i}' as seen_app_{i}_bcast"
            else:
                sel_sa = ""


            subq = f"""left join 
            /* START sql for SEEN APP  */
            (select distinct du.phone sa_phone {sel_sa} {sel_lsd}
            from dw.fact_sms_event fse
            join dw.dim_user du on fse.user_id = du.user_id
            join dw.dim_broadcasts db on db.queue_id = fse.broadcast_queue_id
            join dw.dim_connected_rewards cr on cr.install_promo_id = db.promo_id
            where db.is_broadcast_sent =1 
            and db.active =1
            and db.is_broadcast_canceled =0
            and db.is_test_broadcast = 0
            and du.company_id in ({compid})"""
            if seen_app_i in st:
                subq = subq+f""" and app_title {seen_app_operator} {seen_app_name} {gb_lsd_ad}"""
            else:
                subq = subq+f"""{gb_lsd_d}"""
            subq = subq+f""") {last_seen_date_i}_table
            /* END sql for SEEN APP  */
            on {last_seen_date_i}_table.sa_phone = users.phone """
            op_cond.append(subq)
            
        if (seen_app_i in st) or (broadcast_date_i in st):
            subq = f"""left join 
            /* START sql for seen app or/and Broadcast Date  */
            (select distinct du.phone bc_phone, 'seen_app' as seen_app_{i}, 'broadcast_date' as broadcast_date_{i}
            from dw.fact_sms_event fse
            join dw.dim_user du on fse.user_id = du.user_id
            join dw.dim_broadcasts db on db.queue_id = fse.broadcast_queue_id
            join dw.dim_connected_rewards cr on cr.install_promo_id = db.promo_id
            where db.is_broadcast_sent =1 
            and db.active =1
            and db.is_broadcast_canceled =0
            and db.is_test_broadcast = 0
            and du.company_id in ({compid})"""
            if seen_app_i in st:
                seen_app_name = selection_dict[seen_app_i]['value']
                seen_app_operator = selection_dict[seen_app_i]['new_operator']
                seen_app_operator = reverse_neg_operator(seen_app_operator, negative_operators)
                subq = subq+f""" and cr.app_title {seen_app_operator} {seen_app_name}  """
            if broadcast_date_i in st:
                broadcast_date_filter_val = selection_dict[broadcast_date_i]['value']
                bcast_operator = selection_dict[broadcast_date_i]['new_operator']
                bcast_operator = reverse_neg_operator(bcast_operator,negative_operators) 
                #broadcast_date_filter_val = broadcast_date.replace("(","").replace(")","")
                subq = subq+f""" and db.broadcast_send_date {bcast_operator} {broadcast_date_filter_val}  """
            subq = subq+f""") {broadcast_date_i}_table
            /* END sql for Broadcast Date  */
            on {broadcast_date_i}_table.bc_phone = users.phone """
            if not (seen_app_i in st):
                #subq = subq.replace("""'seen_app' as seen_app,"""," ")
                op_cond.append(subq)
                inner_filter_fields.append(broadcast_date_i)
            else:
                op_cond.append(subq)
                inner_filter_fields.append(seen_app_i)
                inner_filter_fields.append(broadcast_date_i)
        i+=1
        seen_app_i = 'seen_app_'+str(i)
        last_seen_date_i = 'last_seen_date_'+str(i)
        broadcast_date_i = 'broadcast_date_'+str(i)
        #logging_function(log_level='INFO',log_message=f'end: sql_for_seen_app_and_dates')
    return op_cond,inner_filter_fields

def make_select_str_nvl(selection_dict):
    op_str = ''
    for k in list(selection_dict.keys()):
        if selection_dict[k]['field'] in ['filter_add_phone_list',
                                          'filter_add_test_phone_list',
                                            'filter_remove_phone_list',
                                            'filter_intersect_phone_list','exclude_phone_list_from_older_query',
                                            'add_phone_list_from_older_query',
                                            'intersect_phone_list_from_older_query']:
            pass
        elif 'install_likelihood' in k:
            op_str = op_str+', '+ f" nvl({k},false) {k}"
        elif 'zipcode' in k:
            op_str = op_str+', '+ f" nvl(users.zip,'unknown') {k}"
        #elif 'opt_in_source' in k:
        #    op_str = op_str+', '+ f" nvl(subs.opt_in_source,'unknown') {k}"
        elif 'last_seen_date' in k:
            op_str = op_str+', '+ f" nvl(to_char({k},'YYYY-MM-DD'),'unknown') {k}"
        elif selection_dict[k]['valueType'] == 'number':
            op_str = op_str+', '+ f" nvl({k},null) {k}"
        elif selection_dict[k]['valueType'] == 'boolean':
            op_str = op_str+', '+ f" nvl({k},false) {k}"
        else:
            op_str = op_str+', '+ f" nvl({k},'unknown') {k}"
    return op_str

def get_op_relative_directory(compid=0, version=0, query_tag=''):
    op_rel_directory = f"flight_datascience/blb_app_op/{compid}/{query_tag}_{version}"
    #op_rel_directory = f"flight_datascience/blb_app_op/older_queries/{query_tag}_{version}"
    return op_rel_directory

def print_df_info(df):
    print('shape is ',df.shape)
    print('dtypes are\n',df.dtypes)
def get_db_secret():
    try:
        secret_name = "builderapp_db"
        region_name = "us-west-2"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name,
            )
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        secret = get_secret_value_response['SecretString']
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e
    return secret

def query_db(query,bucket, key, print_info = True, print_query = False):
    if print_query:
        print(query)
    db_user = app_config['db_user']
    db_pass = app_config['db_pass']
    #print(db_user,db_pass)
    if db_user=='':
        sec = get_db_secret()
        sec = json.loads(sec)
        db_user = sec['username']
        db_pass = sec['password']
    
    conn = redshift_connector.connect(
        host="""dev-dwh-cluster.cmuwp5enko6p.us-east-1.redshift.amazonaws.com""",
        database='dev',
        user= db_user, 
        password= db_pass
    )
    cursor = conn.cursor()
    res = cursor.execute(query)
    cursor.close()
    conn.close()
    #except:
    #cursor.close()
    #conn.close()
    df = res.fetch_dataframe()
    
    if print_info:
        print_df_info(df)

    return df

# def query_db(query,bucket, key, print_info = True, print_query = False):
#     try:
#         if print_query:
#             print(query)
#         #file_content = content_object.get()['Body'].read().decode('utf-8')
#         cred = s3_read_text_file(bucket, key)
#         sec = json.loads(cred)
#         #print(sec)
#         #try:
#         conn = redshift_connector.connect(
#             host=app_config['db_url'],
#             database=app_config['database_name'],
#             user= sec['USER'], 
#             password= sec['PASSWORD']
#         )
#         cursor = conn.cursor()
#         res = cursor.execute(query)
#         cursor.close()
#         conn.close()
#         #except:
#         #cursor.close()
#         #conn.close()
#         df = res.fetch_dataframe()
        
#         if print_info:
#             print_df_info(df)
#     except Exception as e:
#         error_str = traceback.format_exception_only(e)
#         error_str = ' '.join(error_str).replace('\n',' ')
#         logger.error(error_str)
#         raise e
#     return df

def get_s3_client():
    try:
        access_key = app_config['s3_access_key']
        secret_access_key = app_config['s3_secret_access_key']
        if access_key=='':
            s3_client = boto3.client("s3")
        else:
            s3_client = boto3.client("s3",aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
    return s3_client

def get_smart_suite_s3_client():
    try:
        access_key = app_config['s3_ss_access_key']
        secret_access_key = app_config['s3_ss_secret_access_key']
        if access_key=='':
            s3_client = boto3.client("s3")
        else:
            s3_client = boto3.client("s3",aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
    return s3_client

def upload_json_tos3(data, bucket, filename_key):
    try:
        if s3_comm_on:
            s3_client = get_s3_client()
            s3_client.put_object(
                Body = json.dumps(data),
                Bucket=bucket, Key=filename_key)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e
    
def upload_text_tos3(data, bucket, filename_key):
    try:
        if s3_comm_on:
            s3_client = get_s3_client()
            s3_client.put_object(
                Body = data,
                Bucket=bucket, Key=filename_key)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e

def read_csv(bucket, file_key):
    try:
        s3_client = get_s3_client()
        obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
    return df

def morph_zipcode_query(field, operator,value,valueType, data):
    compid = data['compid']
    query_tag = data['query_tag']
    version = data['VERSION']
    op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
    filter_include_zipcode_file = f'{op_directory}/filter_include_zipcode.csv'
    filter_exclude_zipcode_file = f'{op_directory}/filter_exclude_zipcode.csv'
    print (filter_exclude_zipcode_file,filter_include_zipcode_file )
    bucket =  "mobivity-datascience"
    if (field == 'filter_include_zipcode' ) and value:
        filter_include_zipcode = read_csv(bucket, filter_include_zipcode_file)
        value=list(filter_include_zipcode['zipcode'].astype('Int64').unique())
        field = 'zipcode'
        operator = 'select_any_in'
        valueType='multiselect'
    if (field == 'filter_exclude_zipcode' ) and value:
        filter_exclude_zipcode = read_csv(bucket, filter_exclude_zipcode_file)
        value=list(filter_exclude_zipcode['zipcode'].astype('Int64').unique())
        operator = 'select_not_any_in'
        valueType='multiselect'
        field = 'zipcode'
    return field, operator, value, valueType

def melt_tree(data,compid,
              where_condition_str,
              sql_modular_queries,
              selection_dict,
              negative_operators,
              field_condition_counter,
              inner_filter_fields
              ):
    try:
        logger.info('start')
        ##logging_function(log_level='INFO',log_message=f'start: melt_tree')
        file_based_fields = ['filter_add_phone_list','filter_add_test_phone_list',
                                                'filter_remove_phone_list',
                                                'filter_intersect_phone_list','exclude_phone_list_from_older_query',
                                                'add_phone_list_from_older_query',
                                                'intersect_phone_list_from_older_query']
        where_condition_str = ''
        tree = data['tree']
        if tree['type']=='group':
            where_condition_str = where_condition_str+'('

            if 'properties' in list(tree.keys()):
                if 'conjunction' in list(tree['properties'].keys()):
                    conjucture = tree['properties']['conjunction']
                else:
                    conjucture = 'AND'
                if 'not' in list(tree['properties'].keys()):
                    not_flag = tree['properties']['not']
                else:
                    not_flag=False
            else:
                conjucture='AND'
                not_flag=False
            if 'children' in list(tree.keys()):
                rules = tree['children']
                for rule in rules:
                    if rule['type']=='group':
                        inside_melt_data = data
                        inside_melt_data['tree'] = rule
                        inner_where_condition_str, sql_modular_queries, inner_filter_fields = melt_tree(inside_melt_data,
                                                                                compid,
                                                                                where_condition_str,
                                                                                sql_modular_queries,
                                                                                selection_dict,
                                                                                negative_operators,
                                                                                field_condition_counter,
                                                                                inner_filter_fields)
                        where_condition_str = where_condition_str+' '+inner_where_condition_str
                        where_condition_str = where_condition_str+' '+conjucture
                    elif rule['type'] == 'rule':
                        field = rule['properties']['field']
                        operator = rule['properties']['operator']
                        if not field in file_based_fields:
                            if len(rule['properties']['value'])>0:
                                if operator in ['between','not_between']:
                                    if 'date' in field:
                                        value = str(rule['properties']['value'][0]) +"' and '"+ str(rule['properties']['value'][1])
                                    else:
                                        value = str(rule['properties']['value'][0]) +' and '+ str(rule['properties']['value'][1])
                                else:
                                    value = str(rule['properties']['value'][0])
                                valueType = rule['properties']['valueType'][0]
                            else:
                                value=''
                                valueType=''
                            print(field, operator, value)
                            if field is not None:
                                if 'zipcode' in field:
                                    field, operator, value, valueType = morph_zipcode_query(field, operator,value,valueType, data)   
                                if valueType=='boolean':
                                    value = value.lower()
                                #if valueType=='select':
                                #    value = f"\'{value}\'"
                                if valueType=='multiselect':
                                    value = str(value).replace('[','(').replace(']',')')
                                new_field_name = field_condition_count_func(field,field_condition_counter)
                                new_operator = operator_correction(operator)
                                new_value = sql_dtype_correction(field,operator,value)
                                
                                selection_dict[new_field_name] = {'field':field,'operator':operator,'new_operator':new_operator,'value':new_value, 'valueType':valueType}
                                #print(new_field_name)
                                sql_module_op = sql_modules(field,new_field_name,new_operator, new_value, compid,inner_filter_fields,negative_operators)
                                logger.info('end: sql_modules')
                                if not sql_module_op is None:
                                    sql_modular_queries_temp,inner_filter_fields=sql_module_op
                                    sql_modular_queries.append(sql_modular_queries_temp)
                                    #inner_filter_fields.append(inner_filter_temp)
                                #where_condition_str = where_condition_str+' '+field+' '+operator+' '+value
                                where_condition_str = where_condition_str+' '+get_sql_condition(field,new_field_name,new_operator, new_value)
                                where_condition_str = (not_flag*'NOT')+' '+where_condition_str+' '+conjucture
            where_condition_str = where_condition_str+')'
            where_condition_str = where_condition_str.replace('OR)',')')
            where_condition_str = where_condition_str.replace('AND)',')')
            where_condition_str = where_condition_str.replace('NOT NOT','NOT')
        where_condition_str = where_condition_str.replace('[','(')
        where_condition_str = where_condition_str.replace(']',')')
        
        #where_condition_str = where_condition_str.replace("()","")
        #logging_function(log_level='INFO',log_message=f'end: melt_tree')
        logger.info('start')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e
    return where_condition_str,sql_modular_queries,inner_filter_fields

def build_sql_query_from_tree(data,compid):
    #logging_function(log_level='INFO',log_message=f'start: build_sql_query_from_tree')
    inner_filter_fields = []
    where_condition_str = ''
    sql_modular_queries = []
    selection_dict = {}
    inner_filter_fields=[]
    tree = data['tree']
    field_condition_counter = {}
    inner_filter_fields = []

    where_condition_str, sql_modular_queries, inner_filter_fields = melt_tree(data,
                                                                              compid,
                                                                              where_condition_str,
                                                                              sql_modular_queries,
                                                                              selection_dict,
                                                                              negative_operators,
                                                                              field_condition_counter,
                                                                              inner_filter_fields)
    
    sql_modular_queries, inner_filter_fields = sql_for_seen_app_and_dates(selection_dict,
                                                                          sql_modular_queries, 
                                                                          compid,inner_filter_fields)

    for f in list(selection_dict.keys()):
        if f in inner_filter_fields:
            made_up_query = f"{f} {selection_dict[f]['new_operator']} {selection_dict[f]['value']}"
            new_f = selection_dict[f]['field']
            if selection_dict[f]['new_operator'] in list(negative_operators.keys()):
                fin_op_operator = '!='
            else:
                fin_op_operator = '='
            new_out_query = f"{f} {fin_op_operator} '{new_f}'"
            where_condition_str = where_condition_str.replace(made_up_query,new_out_query)

    for f in list(selection_dict.keys()):
        if f in inner_filter_fields:
            if 'broadcast_date' in f:
                i = f.split('broadcast_date_')[-1]
                seen_tag = 'seen_app_'+i+' !='
                if seen_tag in where_condition_str:
                    where_condition_str = where_condition_str.replace('broadcast_date_'+i+' =','broadcast_date_'+i+' !=')
                seen_tag = 'seen_app_'+i+' ='
                if seen_tag in where_condition_str:
                    where_condition_str = where_condition_str.replace('broadcast_date_'+i+' !=','broadcast_date_'+i+' =')

    additional_select_str = ', '.join(list(selection_dict.keys()))
    
    additional_select_str = make_select_str_nvl(selection_dict)
    join_queries = ' '.join(sql_modular_queries)
    zip_select_str = ''

    fin_sql = f"""
        select distinct phone from
        (
        select distinct users.phone {additional_select_str} {zip_select_str} from
        dw.dim_user users
        join dw.dim_subscription subs on subs.user_id = users.user_id 
        join dw.dim_campaign camps on camps.campaign_id = subs.campaign_id 
        join dw.dim_company company on company.companyid = camps.companyid and users.company_id = company.companyid {join_queries}
        where subs.is_subscribed_flag =1
        and subs.is_double_optin_flag =1
        and camps.is_active_flag =1
        and camps.is_campaign_hidden_flag = 0
        and users.company_id in ({compid})
        and users.phone is not null)
        where {where_condition_str}
        """
    fin_sql  = fin_sql.replace("where ()","")
    fin_sql  = fin_sql.replace("AND ()","")
    fin_sql  = fin_sql.replace("OR ()","")
    fin_sql  = fin_sql.replace("AND )","")
    fin_sql  = fin_sql.replace("OR )","")
    fin_sql  = fin_sql.replace("NOT ()","")
    fin_sql  = fin_sql.replace("NOT )","")
    fin_sql = fin_sql.replace("Checkers & Rally's","Checkers & Rally''s")
    fin_sql = fin_sql.replace('"',"'")
    #logging_function(log_level='INFO',log_message=f'end: build_sql_query_from_tree')
    
    return fin_sql

class FindBestMatch():
    def __init__(self, word, word_list):
        self.word = word
        self.word_list = word_list
    def find(self):
        match_list = get_close_matches(self.word, self.word_list)
        if len(match_list)>0:
            return match_list[0]
        else:
            return None
    def find_game(self):
        match_list = get_close_matches(self.word, self.word_list)
        if len(match_list)>0:
            return match_list[0]
        else:
            return f'New Game ({self.word})'
        
    def find_os(self):
        match_list = get_close_matches(self.word, self.word_list)
        if len(match_list)>0:
            return match_list[0]
        else:
            return 'All'

class RuleTree():
    def __init__(self, base_tree, field, operator, value,):
        self.base_tree = base_tree
        self.field = field
        self.operator = operator
        self.value = value
        self.tree_rule_operator_reverse_map = {   
                                    '=':'equal',
                                    '!=':'select_not_equals',
                                    '>':'greater',
                                    '>=':'greater_or_equal',
                                    '<':'less',
                                    '<=':'less_or_equal',
                                }
    def build_tree_all_and_rules(self):
        if not self.base_tree:
            self.base_tree = {'type': 'group', 'properties': {'conjunction': 'AND', 'not': False}, 'children': []}
        if type(self.value)==bool:
            valueType = 'boolean'
        elif type(self.value)==str:
            valueType = 'select'
        elif type(self.value)==list:
            valueType = 'multiselect'
        self.tree_based_operator = self.tree_rule_operator_reverse_map[self.operator]
        self.child = {'type': 'rule', 'properties': {'fieldSrc': 'field', 'field': self.field, 'operator': self.tree_based_operator, 'value': [self.value], 'valueSrc': ['value'], 'valueType': [valueType]}}
        if 'children' not in self.base_tree.keys():
            self.base_tree['children'] = self.child
        else:
            self.base_tree['children'].append(self.child)
        return self.base_tree

def decode_bcast_url_response(url_reposnse):
                    results = io.BytesIO(url_reposnse.content)
                    wrapper = io.TextIOWrapper(results, encoding='utf-8')
                    results = wrapper
                    results = json.load(wrapper)
                    results = json.loads(results)
                    return results

def create_rule_tree(most_like_flag='',device_os='',seen_app='',installed_app='',broadcast_date=''):
    child_rule_list = []
    if seen_app!='':
        seen_app_rule = {'type': 'rule', 'properties': {'fieldSrc': 'field', 'field': 'seen_app', 'operator': 'select_equals', 'value': [seen_app], 'valueSrc': ['value'], 'valueType': ['select']}}
        child_rule_list.append(seen_app_rule)
    if installed_app!='':
        installed_app_rule = {'type': 'rule', 'properties': {'fieldSrc': 'field', 'field': 'installed_app', 'operator': 'select_not_equals', 'value': [installed_app], 'valueSrc': ['value'], 'valueType': ['select']}}
        child_rule_list.append(installed_app_rule)
    if most_like_flag!='':
        most_likely_rule = {'type': 'rule', 'properties': {'fieldSrc': 'field', 'field': 'install_likelihood', 'operator': 'equal', 'value': [most_like_flag], 'valueSrc': ['value'], 'valueType': ['boolean']}}
        child_rule_list.append(most_likely_rule)
    if broadcast_date!='':
        bcast_date_rule = {'type': 'rule', 'properties': {'fieldSrc': 'field', 'field': 'broadcast_date', 'operator': 'greater_or_equal', 'value': [broadcast_date], 'valueSrc': ['value'], 'valueType': ['date']}}
        child_rule_list.append(bcast_date_rule)
    if not (device_os.lower() in ['all','']) :
        os_rule = {'type': 'rule', 'properties': {'fieldSrc': 'field', 'field': 'device_os', 'operator': 'select_equals', 'value': [device_os], 'valueSrc': ['value'], 'valueType': ['select']}}
        child_rule_list.append(os_rule)
    tree = {'type': 'group', 'properties': {'conjunction': 'AND', 'not': False}, 'children': child_rule_list}
    return tree

def check_phone_num_length(phone_series, return_series = False):
    len_ser = phone_series.apply(lambda x: len(str(x)))
    #print(len_ser.value_counts())
    if return_series:
        return len_ser
    else:
        return None


def build_query(st, version,fields_dict, OP_SQL_CODE ,OP_PHONE_COUNT, OP_CSV_FILE, data):
    try:
        logger.info('start: build_query')
        ##logging_function(log_level='INFO',log_message=f'start: build_query')
        compid = data['compid']
        tree = data['tree']
        sql_query = build_sql_query_from_tree(data,compid)
        logger.info('end: build_query')
        ##logging_function(log_level='INFO',log_message=f'end: build_query')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        sql_query = error_str
        logger.error(error_str)
    
    return sql_query


def read_csv(bucket, file_key):
    try:
        logger.info('start: read_csv')
        s3_client = get_s3_client()
        obj = s3_client.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        logger.info('end: read_csv')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise(e)
    return df

def s3_read_text_file(bucket, key):
    try:
        logger.info('start: s3_read_text_file')
        s3_client =get_s3_client()
        resp = s3_client.get_object(Bucket = bucket,Key = key)
        obj_cont = resp['Body'].read().decode('utf-8')
        logger.info('end: s3_read_text_file')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
    return obj_cont







def split_df_multipe_file_and_save(df,add_test_phone,divs,bucket,file_name, version,data):
    try:
        logger.info('start')
        #logging_function(log_level='INFO',log_message=f'split_df_multipe_file_and_save')
        compid = data['compid']
        query_tag = data['query_tag']
        file_name_wo_csv = file_name.split('.csv')[0]
        ss_directory = f"FilterLists/CID-{compid}/{query_tag}_{version}"
        ss_fname = file_name_wo_csv.split("/")[-1]
        ss_full_name = f"{ss_directory}/{ss_fname}"
        fin_op=df[['phone']].drop_duplicates().reset_index(drop=True)
        n = fin_op.shape[0]
        df_return = []
        if n==0:
            save_file_name = f'{file_name_wo_csv}.csv'
            upload_csv_tos3(fin_op, bucket, save_file_name)
            ss_full_name_csv = f'{ss_full_name}.csv'
            upload_csv_to_smart_suite_s3(fin_op, ss_full_name_csv, dep_environment)
            df_return.append(fin_op)
            full_file_name = f"s3://{bucket}/{save_file_name}"
        start = 0
        end=0
        each_size = int(round(n/divs))
        i=0
        op_filenames_list = []
        phone_count_list= []
        if df.shape[0]<1:
            phone_count_list.append('0')
        if add_test_phone:
            op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
            filter_add_test_phone_list_file = f'{op_directory}/filter_add_test_phone_list.csv'
            filter_add_test_phone_list = read_csv(bucket, filter_add_test_phone_list_file)
            filter_add_test_phone_list['phone'] = filter_add_test_phone_list[['phone']].astype('Int64')
        while end<n:
            end = start+each_size
            temp = fin_op.iloc[start:end]
            start = end
            i+=1
            temp = temp.drop_duplicates()
            if add_test_phone:
                temp = pd.concat([filter_add_test_phone_list,temp])
            if divs>1:
                save_file_name = f'{file_name_wo_csv}_part{i}.csv'
                ss_full_name_csv = f'{ss_full_name}_part{i}.csv'
            else:
                save_file_name = f'{file_name_wo_csv}.csv'
                ss_full_name_csv = f'{ss_full_name}.csv'
            upload_csv_tos3(temp, bucket, save_file_name)
            upload_csv_to_smart_suite_s3(temp, ss_full_name_csv, dep_environment)
            df_return.append(temp)
            full_file_name = f"s3://{bucket}/{save_file_name}"
            op_filenames_list.append(full_file_name)
            phone_count_list.append(str(temp.shape[0]))
            #logging_function(log_level='INFO',log_message=f'split_df_multipe_file_and_save finish')
            logger.info('end')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e
    return phone_count_list, op_filenames_list, df_return

def upload_csv_tolocal(df, folder, filename):
    df.to_csv()




def upload_csv_tos3(df, bucket, filename_key):
    try:
        logger.info('start')
        #logging_function(log_level='INFO',log_message=f'start: upload_csv_tos3')
        if s3_comm_on:
            s3_client = get_s3_client()
            with io.StringIO() as csv_buffer:
                df.to_csv(csv_buffer, index=False)
                response = s3_client.put_object(Bucket=bucket, Key=filename_key, Body=csv_buffer.getvalue() )
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if status != 200:
                    logger.error(f"Unsuccessful S3 put_object response. Status - {status}")
        #logging_function(log_level='INFO',log_message=f'end: upload_csv_tos3')
        logger.info('end')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e
    
def upload_csv_to_smart_suite_s3(df,filename_key, dep_environment):
    try:
        logger.info('start')
        if dep_environment!='local':
            #logging_function(log_level='INFO',log_message=f'start: upload_csv_to_smart_suite_s3')
            bucket = os.environ['recurrency_bucket']  #'smartsuiteqa' , 'smartsuiteprod'
            if s3_comm_on:
                s3_client =  get_smart_suite_s3_client()
                with io.StringIO() as csv_buffer:
                    df.to_csv(csv_buffer, index=False)
                    response = s3_client.put_object(Bucket=bucket, Key=filename_key, Body=csv_buffer.getvalue() )
                    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                    if status != 200:
                        logger.error(f"Unsuccessful S3 put_object response. Status - {status}")
            #logging_function(log_level='INFO',log_message=f'end: upload_csv_to_smart_suite_s3')
        logger.info('end')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e

def read_multiple_csv(bucket, blb_app_ref, data):
    try:
        logger.info('start')
        #logging_function(log_level='INFO',log_message=f'start: read_multiple_csv')
        s3_client = get_s3_client()
        compid = data['compid']
        #print(blb_app_ref)
        query_tag =  blb_app_ref.split('_')[:-1]#data['query_tag']
        query_tag = '_'.join(query_tag)
        version =  blb_app_ref.split('_')[-1] #data['VERSION']
        op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
        full_prefix = f'{op_directory}/query_output_phone_list_{version}'
        #print(full_prefix)
        s3_client.list_objects(Bucket = bucket,Prefix=full_prefix).keys()
        objects = s3_client.list_objects(Bucket = bucket,Prefix=full_prefix)
        file_list  = objects['Contents']
        file_keys = [x['Key'] for x in file_list]
        df_list = []
        for fkey in file_keys: 
            obj = s3_client.get_object(Bucket=bucket, Key=fkey)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            df_list.append(df)
        fin_df = pd.concat(df_list)
        #logging_function(log_level='INFO',log_message=f'end: read_multiple_csv')
        logger.info('end')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e
    return fin_df



def cancel_query(version, bucket):
    try:
        sql_query = """SELECT pid, trim(user_name), starttime, substring(query,1,51) query_str 
                        FROM stv_recents
                        WHERE status='Running'
                        and query_str  ilike '----BLB App Reference Number: {version}%';
                        """
        config_file_key = "install_likelihood_v2/source_code/config.json"

        config_file = s3_read_text_file(bucket, config_file_key)
        config_params = json.loads(config_file)
        print_query = config_params['print_query']
        key = config_params['cred_file_key']
        df = query_db(sql_query,bucket, key, print_info=False)
        print(df)
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e

def delete_older_files(delete_older_than_days , 
                       today,
                       bucket,
                       file_directory,
                       pre_split_format, 
                       post_split_format):
    try:
        today = datetime.datetime.now().date()
        history_delete_date = str(today-datetime.timedelta(days=delete_older_than_days))
        s3_client = get_s3_client()
        my_bucket = s3_client.Bucket(bucket)
        for my_bucket_object in my_bucket.objects.filter(Prefix=file_directory):
            filename_key = my_bucket_object.key
            if pre_split_format in filename_key:  
                date_from_filename = filename_key.split(pre_split_format)[-1]
                file_date = date_from_filename.split(post_split_format)[0]
                if history_delete_date >=file_date:
                    print(filename_key)
                    s3_client.Object(bucket, filename_key).delete()
                    print('deleted')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e


def convert_comma_text_to_list_of_int(text_for_int):
    try:
        logger.info('start')
        if not text_for_int is None:
            text_for_int = text_for_int.split(',')
            list_of_int = [x.strip() for x in text_for_int if x.strip()!='']
        else:
            list_of_int = None
        logger.info('end')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise(e)
    return list_of_int

def execute_streamlit_queries(sql_query, version,bucket, file_name,OP_PHONE_COUNT,OP_CSV_FILE, file_split_number,file_limit_number, data ):
    try:
        logger.info('start')
        logger.info('start')
        config_file_key = "install_likelihood_v2/source_code/blb_app_config.json"

        config_file = s3_read_text_file(bucket, config_file_key)
        config_params = json.loads(config_file)
        compid = data['compid']
        query_tag = data['query_tag']

        ##--------user_inputs-----------
        print_query = config_params['print_query']
        key = config_params['cred_file_key']
        #full_file_name = f"s3://{bucket}/{file_name}"
        phone_counts = None
        add_test_phone = False
        if OP_CSV_FILE or OP_PHONE_COUNT:
            #logging_function(log_level='INFO',log_message=f'db query start')
            df = query_db(sql_query,bucket, key, print_info=False)
            #logging_function(log_level='INFO',log_message=f'db query finish')
            if DEBUG: logger.warning(df.shape)
            op_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
            filter_add_phone_list_file = f'{op_directory}/filter_add_phone_list.csv'
            filter_remove_phone_list_file = f'{op_directory}/filter_remove_phone_list.csv'
            filter_intersect_phone_list_file = f'{op_directory}/filter_intersect_phone_list.csv'

            #logging_function(log_level='INFO',log_message=f'dealing with extra phone list filters')

            exclude_phone_list_from_older_query = data['exclude_phone_list_from_older_query']
            add_phone_list_from_older_query = data['add_phone_list_from_older_query']
            intersect_phone_list_from_older_query = data['intersect_phone_list_from_older_query']

            exclude_phone_list_from_older_query = convert_comma_text_to_list_of_int(exclude_phone_list_from_older_query)
            add_phone_list_from_older_query = convert_comma_text_to_list_of_int(add_phone_list_from_older_query)
            intersect_phone_list_from_older_query = convert_comma_text_to_list_of_int(intersect_phone_list_from_older_query)

            if DEBUG: logger.warning(f'exclude_phone_list_from_older_query: {exclude_phone_list_from_older_query}')
            if DEBUG: logger.warning(f'intersect_phone_list_from_older_query: {intersect_phone_list_from_older_query}')
            if DEBUG: logger.warning(f'add_phone_list_from_older_query: {add_phone_list_from_older_query}')

            print('p1',intersect_phone_list_from_older_query)
            
            if 'filter_add_phone_list = true' in data['st_query']:
                filter_add_phone_list = read_csv(bucket, filter_add_phone_list_file)
                filter_add_phone_list['phone'] = filter_add_phone_list['phone'].astype('Int64')
                df = pd.concat([df,filter_add_phone_list[['phone']]]).drop_duplicates().reset_index(drop=True)
            if 'filter_remove_phone_list = true' in data['st_query']:
                filter_remove_phone_list = read_csv(bucket, filter_remove_phone_list_file)
                filter_remove_phone_list['phone'] = filter_remove_phone_list['phone'].astype('Int64')
                df = df[~df['phone'].isin(list(filter_remove_phone_list['phone'].unique()))].reset_index(drop=True)
            if 'filter_intersect_phone_list = true' in data['st_query']:
                filter_intersect_phone_list = read_csv(bucket, filter_intersect_phone_list_file)
                filter_intersect_phone_list['phone'] = filter_intersect_phone_list['phone'].astype('Int64')
                df = df[df['phone'].isin(list(filter_intersect_phone_list['phone'].unique()))].reset_index(drop=True)
            if exclude_phone_list_from_older_query is not None:
                temp_exclude_list = []
                for old_query_ref in exclude_phone_list_from_older_query:
                    df_exclude_phone_list_from_older_query = read_multiple_csv(bucket, old_query_ref, data)
                    temp_exclude_list.append(df_exclude_phone_list_from_older_query)
                df_exclude_phone_list_from_older_query = pd.concat(temp_exclude_list)
                df_exclude_phone_list_from_older_query['phone'] = df_exclude_phone_list_from_older_query['phone'].astype('Int64')
                if DEBUG: logger.warning(f'size of exclusion from older query is : {df_exclude_phone_list_from_older_query.shape}')
                df = df[~df['phone'].isin(list(df_exclude_phone_list_from_older_query['phone'].unique()))].reset_index(drop=True)
            if intersect_phone_list_from_older_query is not None:
                temp_exclude_list = []
                for old_query_ref in intersect_phone_list_from_older_query:
                    df_intersect_phone_list_from_older_query = read_multiple_csv(bucket, old_query_ref, data)
                    temp_exclude_list.append(df_intersect_phone_list_from_older_query)
                
                df_intersect_phone_list_from_older_query = pd.concat(temp_exclude_list)
                if DEBUG: logger.warning(f'size of intersect from older query is : {df_intersect_phone_list_from_older_query.shape}')
                df_intersect_phone_list_from_older_query['phone'] = df_intersect_phone_list_from_older_query['phone'].astype('Int64')
                df = df[df['phone'].isin(list(df_intersect_phone_list_from_older_query['phone'].unique()))].reset_index(drop=True)
            if add_phone_list_from_older_query is not None:
                temp_exclude_list = []
                for old_query_ref in add_phone_list_from_older_query:
                    df_add_phone_list_from_older_query = read_multiple_csv(bucket, old_query_ref, data)
                    temp_exclude_list.append(df_add_phone_list_from_older_query)
                df_add_phone_list_from_older_query = pd.concat(temp_exclude_list)
                df_add_phone_list_from_older_query['phone'] = df_add_phone_list_from_older_query['phone'].astype('Int64')
                df = pd.concat([df,df_add_phone_list_from_older_query[['phone']]]).drop_duplicates().reset_index(drop=True)
            if 'filter_add_test_phone_list' in data['st_query']:
                
                add_test_phone = True
                

            if file_limit_number>0:
                if file_limit_number<=df.shape[0]:
                    df = df.sample(n=file_limit_number, random_state=7).reset_index(drop=True)

        if OP_CSV_FILE or OP_PHONE_COUNT:
            phone_count_list, full_file_names, df_return = split_df_multipe_file_and_save(df,add_test_phone,file_split_number,bucket,file_name, version, data)
            #upload_csv_tos3(df, bucket, file_name)
        logger.info('end')
    except Exception as e:
        error_str = traceback.format_exception_only(e)
        error_str = ' '.join(error_str).replace('\n',' ')
        logger.error(error_str)
        raise e
        
    return phone_count_list,full_file_names, df_return

def function_for_st_query(data):
    resp = {}
    bucket =  app_config['bucket']
    version = data['VERSION']
    query_str = data['st_query']
    OP_SQL_CODE  = data['OP_SQL_CODE']
    OP_PHONE_COUNT = data['OP_PHONE_COUNT']
    OP_CSV_FILE = data['OP_CSV_FILE']
    share_field_dict = data['share_field_dict']
    file_limit_number = int(data['file_limit_number'])
    file_split_number = int(data['file_split_number'])

    compid = data['compid']
    user = data['user']
    query_tag = data['query_tag']
    logger.warning(f'New Query for compid {compid}. {query_tag}, {version}, {user}')
    file_directory = get_op_relative_directory(compid=compid, version=version, query_tag=query_tag)
    #f'flight_datascience/blb_app_op/{compid}/{query_tag}_{version}'

    
    st=query_str
    print('OP_PHONE_COUNT',OP_PHONE_COUNT, 'OP_CSV_FILE',OP_CSV_FILE, 'OP_SQL_CODE',OP_SQL_CODE)
    file_name = f'{file_directory}/query_output_phone_list_{version}.csv'

    prefix_fin_query = ""

    if OP_PHONE_COUNT or OP_CSV_FILE or OP_SQL_CODE:
        if 'sql_query' in data.keys():
            sql_query = data['sql_query']
        else:
            original_sql_tree = data['tree']
            sql_query = build_query(st, version,share_field_dict, OP_SQL_CODE,OP_PHONE_COUNT, OP_CSV_FILE, data) 
        if DEBUG: logger.warning(sql_query)
    else:
        fin_query = "Please select atleast one of the output type!"
        

    if OP_PHONE_COUNT or OP_CSV_FILE : 
        phone_counts,full_file_names, df_return = execute_streamlit_queries(sql_query, 
                                                                version,bucket, 
                                                                file_name,
                                                                OP_PHONE_COUNT,
                                                                OP_CSV_FILE, file_split_number,
                                                                file_limit_number , data
                                                                )
        resp['phone_count'] = phone_counts
        resp['op_file_names'] = full_file_names

    if OP_PHONE_COUNT :
        phone_counts = " , ".join(phone_counts)
        phone_count_text = f"""
/*pc_starts
Total Unique Phone Numbers are {phone_counts}
pc_ends*/
"""
        prefix_fin_query = prefix_fin_query+phone_count_text

    if OP_CSV_FILE:
        full_file_name = " ,\\n".join(full_file_names)
        file_text=''
        if phone_counts[0]!='0':
            file_text = f"""/*fn_starts
Results of Above Query Available in 
{full_file_name}
fn_ends*/
"""
        prefix_fin_query = prefix_fin_query+file_text
    else:
        full_file_names = []
        df_return = []
    if OP_SQL_CODE:
        fin_query = prefix_fin_query+sql_query
    else:
        fin_query = prefix_fin_query
    query_all_params_fname = f'{file_directory}/query_all_params_{version}.json'
    if OP_SQL_CODE==0:
        data['sql_query'] = fin_query+sql_query
        data['sql_query']  = data['sql_query'].replace("\\n"," ").replace("\n"," ")
    else:
        data['sql_query'] = fin_query.replace("\\n"," ").replace("\n"," ")
    
    data['tree'] = original_sql_tree
    upload_json_tos3(data, bucket, query_all_params_fname)
    resp['fin_query'] = fin_query

    for i in range(len(full_file_names)):
        temp_df = df_return[i]
        return_fname = full_file_names[i]
        stringio  = io.StringIO()
        temp_df.to_csv(stringio, index=False,header=False)
        resp[return_fname] = stringio.getvalue()
    return resp