import hmac
from streamlit_condition_tree import condition_tree
import streamlit as st
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
pd.options.display.max_columns=None
import requests
import datetime
import zipfile
import io
from io import BytesIO
import time
import json
import requests, traceback
import os
from blb_functions import create_rule_tree, RuleTree, decode_bcast_url_response, logging_function,dep_environment,APP_VERSION,DEBUG
from difflib import get_close_matches

st.set_page_config(layout="wide", page_title=f'BRIE.{APP_VERSION}',page_icon='dependencies/mobi_logo.png')

with open('config_blb_app.json') as json_file:
    app_config = json.load(json_file)

def check_password():
    """Returns `True` if the user had a correct password."""

    def login_form():
        """Form with widgets to collect user information"""
        with st.form("Credentials"):
            st.markdown('<h2 style="text-align: center;font-family:Tahoma; color:white;background-color: #8dc63f">BRIE</h2>', unsafe_allow_html=True)
            st.markdown('<h4 style="text-align: center;font-family:Tahoma; color:white;background-color: #8dc63f">(Broadcast Revenue & Impressions Estimator)</h4>', unsafe_allow_html=True)
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)
        st.markdown(f'<p style="text-align: right;font-family:Tahoma; font-size:12px;color:black; position: relative; top:0px;">{APP_VERSION}</p>', unsafe_allow_html=True)

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if not "persistent_username" in st.session_state:
                  st.session_state.persistent_username = 'None'
        if st.session_state["username"] in st.secrets[
            "passwords"
        ] and hmac.compare_digest(
            st.session_state["password"],
            st.secrets.passwords[st.session_state["username"]],
        ):
            st.session_state.persistent_username = st.session_state["username"]
            st.session_state["password_correct"] = True
            logging_function(log_level='WARNING',log_message=f'{st.session_state.persistent_username} login detected in BRIE')
            del st.session_state["password"]  # Don't store the username or password.
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False
            st.session_state.persistent_username = st.session_state["username"]

    # Return True if the username + password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show inputs for username + password.
    login_form()
    if "password_correct" in st.session_state:
        logging_function(log_level='WARNING',log_message=f'{st.session_state.persistent_username} login Failed')
        st.error("😕 User not known or password incorrect")
    return False


if not check_password():
    st.stop()
#st.markdown(f'<p style="text-align: right;font-family:Tahoma; font-size:12px;color:black;">{APP_VERSION} Signed in as {st.session_state.persistent_username}</p>', unsafe_allow_html=True)

hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.css-hi6a2p {padding-top: 0rem;}
</style>

"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 


st.markdown(
    """
<style>
div[class^="block-container"] {
    padding-top: 0rem;
}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown("""
        <style>
               .block-container {
                    padding-top: 0rem;
                    padding-bottom: 5rem;
                    padding-left: 5rem;
                    padding-right: 5rem;
                }
        </style>
        """, unsafe_allow_html=True)
st.markdown(f'<p style="text-align: right;font-family:Tahoma; font-size:12px;color:black; position: relative; top:10px;">{APP_VERSION} Signed in as {st.session_state.persistent_username}</p>', unsafe_allow_html=True)
st.html('<h2 style="text-align: center;font-family:Tahoma; color:white;background-color: #8dc63f;border-radius: 10px;position:relative;left:0px;right:0px;">BRIE</h2>')

if 'fetch_clicked' not in st.session_state:
    st.session_state.fetch_clicked = False


#st.markdown('<h2 style="text-align: center;font-family:Tahoma; color:black;">BRIE</h2>', unsafe_allow_html=True)
df_comp = pd.read_csv('dependencies/company_names.csv')
df_app = pd.read_csv('dependencies/app_titles.csv')
df_app = df_app.dropna()

app_title_list = list(df_app['app_title'])
app_title_list = list(df_app['app_title'])
new_game_holder = ['New Game 1', 'New Game 2','New Game 3','New Game 4', 'New Game 5']
app_title_list = new_game_holder + app_title_list
device_os_list = ['iOS','Android','All']

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
            return 'New Game x'
        
    def find_os(self):
        match_list = get_close_matches(self.word, self.word_list)
        if len(match_list)>0:
            return match_list[0]
        else:
            return 'All'

game_plan_input_table_config ={
                "app_title": st.column_config.SelectboxColumn(
                    "app_title",
                    width="medium",
                    options=app_title_list,
                    required=True),
                'cpi':st.column_config.NumberColumn(
                    "cpi",
                    min_value=0,
                    format="$ %.2f",
                    required=True),
                'priority':st.column_config.SelectboxColumn(
                    "priority",
                    options=[1,2,3,4,5,6,7,8,9,10],
                    required=True),
                'device_os':st.column_config.SelectboxColumn(
                    "device_os",
                    options=device_os_list,
                    required=True),
                'estimated_install_rate':st.column_config.NumberColumn(
                    "estimated_install_rate",
                    min_value=0,
                    format="%.2f%%",
                    required=True),
                    }

comp_placeholder = st.empty()
company_filter = comp_placeholder.selectbox(label='Filter a brand', index=None, options=list(df_comp['company_names'].unique()))
if not company_filter is None:
    compid = int(df_comp[df_comp['company_names']==company_filter].reset_index(drop=True)['comp_id'][0])
else:
    compid=0
broadcast_date_placeholder = st.empty()
broadcast_date_filter = broadcast_date_placeholder.date_input(label='Ignore Broadcasts before:',format='YYYY-MM-DD')
active_game_list = {'Woodoku': {'cpi':2.4, 'priority':6,'device_os':'All'},
                     'Triple Tile': {'cpi':2.4, 'priority':5,'device_os':'All'},
                     'Tiki Solitaire TriPeaks':  {'cpi':3.08, 'priority':4,'device_os':'All'},
                     'Cash Me Out Bingo':  {'cpi':6.13, 'priority':1,'device_os':'iOS'},
                     'Rewarded Play': {'cpi':5.5, 'priority':2,'device_os':'Android'},
                     'Monopoly Go!': {'cpi':4, 'priority':3,'device_os':'All'},}
active_game_df2 = pd.DataFrame(active_game_list).T
active_game_df2=active_game_df2.reset_index().rename(columns = {'index':'app_title'})
active_game_df2 = active_game_df2.sort_values(by = 'priority').reset_index(drop=True)
active_game_df2['estimated_install_rate']=0.0
app_plan_df = active_game_df2.head(1)
df = pd.read_csv('dependencies/app_titles.csv')
df = df.dropna()

lock_variable=False


if not lock_variable:
    table_input_col1, table_input_col2  = st.columns([0.65,0.35], gap='small')
    with table_input_col1:
        st.write('Enter the available apps/games and their variables')
        input_table_placeholder = st.empty()
    app_plan_df = input_table_placeholder.data_editor(app_plan_df,num_rows='dynamic', key='n1',
                                    column_config= game_plan_input_table_config)
    with table_input_col2:
        #st.markdown('<p style="text-align: left; box-sizing: border-box;color:black;">OR Download the table on left, edit it and upload as csv file</p>', unsafe_allow_html=True)
        app_plan_df_uploaded = st.file_uploader('OR Download the table on left, edit it and upload as csv file',key='game_table_uploader_element')
        if app_plan_df_uploaded is not None:
            app_plan_df = pd.read_csv(app_plan_df_uploaded)
            for col_name in list(app_plan_df.columns):
                if 'unnamed' in col_name.lower():
                    app_plan_df = app_plan_df.drop(columns=[col_name])
            app_plan_df['app_title']=app_plan_df['app_title'].apply(lambda x: FindBestMatch(x, app_title_list).find_game())
            app_plan_df['device_os']=app_plan_df['device_os'].apply(lambda x: FindBestMatch(x, device_os_list).find_os())
            input_table_placeholder.data_editor(app_plan_df,num_rows='fixed',disabled=False,
                                column_config= game_plan_input_table_config, key='n2' )
lcol1, lcol2,lcol3,lcol4 = st.columns(4)

with lcol1:
    lock_switch_ph = st.empty()
with lcol2:
    st.write(' ')
with lcol3:
    st.write(' ')
lock = lock_switch_ph.toggle(label='Commit variable?', disabled=False)
if lock:
    lock_variable=True
if lock_variable:
    comp_placeholder.selectbox(label='Filter a brand', index=0,disabled=True, options=[company_filter])
    input_table_placeholder.data_editor(app_plan_df,num_rows='fixed',disabled=True,
                                column_config= game_plan_input_table_config,key='n3'
                )
    broadcast_date_placeholder.text_input(label='Ignore Broadcasts before:',placeholder=str(broadcast_date_filter), disabled=True)
#st.write(app_plan_df['estimated_install_rate'][0])
active_game_df = app_plan_df.sort_values(by = 'priority').reset_index(drop=True)
bcast_date_filter=str(broadcast_date_filter)
#st.session_state.fetch_clicked = False

def fetch_click_button():
    st.session_state.fetch_clicked = True

#recalc_button = st.button(label='Recalculate',type='primary')
if company_filter is None:
    st.info('Select Brand Name')
else:
    calc_btn_ph = st.empty()
    recalc_button = calc_btn_ph.button('Calculate', type='primary',on_click=fetch_click_button)
PORT_NUM = app_config['backend_port']
backend_base_url =app_config['backend_url']
url = f"{backend_base_url}:{PORT_NUM}/st_query/"
share_field_dict= {'install_likelihood': '',
 'device_os': '',
 'installed_app': '',
 'seen_app': '',
 'broadcast_date': '',
 'last_seen_date': '',
 'clicked_app': '',
 'state_name': '',
 'bu_name': '',
 'opt_in_source': '',
 'camp_keyword': '',
 'redeemer_flag': '',
 'days_since_last_redemption': '',
 'engaged': '',
 'tenure': '',
 'filter_include_zipcode': '',
 'filter_exclude_zipcode': '',
 'filter_add_phone_list': '',
 'filter_add_test_phone_list': '',
 'filter_remove_phone_list': '',
 'filter_intersect_phone_list': '',
 'exclude_phone_list_from_older_query': '',
 'add_phone_list_from_older_query': '',
 'intersect_phone_list_from_older_query': ''}

outer_dict = {}
priority_sorted_game_list = active_game_df.app_title.unique()
if st.session_state.fetch_clicked:
    if lock_variable:
        st.session_state.fetch_clicked = False
        #st.write('here')
        with st.spinner(text='Processing...'):
            dnrp_ph = st.empty()
            dnrp_ph.info('Do not refresh or close the page!')
            lock_switch_ph.toggle(label='Commit variable?', value=False, disabled=True, key='t2')
            calc_btn_ph.button('Calculate', type='primary',disabled=True, key='btn2')
            iter_count = active_game_df.shape[0]
            query_tag = 'estimator_app'

            # Query for Seen Count
            for curr_iter in range(iter_count):
                current_game_plan_dict = active_game_df.T.to_dict()[curr_iter]
                app_title = current_game_plan_dict['app_title']
                device_os = current_game_plan_dict['device_os']
                priority = current_game_plan_dict['priority']
                VERSION = str(datetime.datetime.now()).replace(':','').replace('-','').replace('.','').replace(' ','')
                blb_ref_num = query_tag+'_'+str(VERSION)

                inner_dict = {}
                query_tree = {}
                query_tree = RuleTree(base_tree=query_tree, field='seen_app', operator='=',value=app_title).build_tree_all_and_rules()
                query_tree = RuleTree(base_tree=query_tree, field='broadcast_date', operator='>=',value=bcast_date_filter).build_tree_all_and_rules()
                if device_os.lower()!='all':
                    query_tree = RuleTree(base_tree=query_tree, field='device_os', operator='=',value=device_os).build_tree_all_and_rules()
                body_data = {'VERSION':VERSION, 
                                    'query_tag':query_tag,
                                    'st_query': '',
                                    'user':st.session_state.persistent_username,
                                    'compid':compid,
                                    'OP_SQL_CODE':0, 
                                    'OP_PHONE_COUNT':1,
                                    'OP_CSV_FILE':1,
                                    'share_field_dict':share_field_dict, 'tree':query_tree,
                                    'file_limit_number':0,'file_split_number':1,
                                    'exclude_phone_list_from_older_query':None,
                                    'add_phone_list_from_older_query':None,
                                    'intersect_phone_list_from_older_query':None,
                                    }
                if DEBUG: logging_function(log_level='WARNING',log_message=query_tree)
                url_reposnse = requests.post(url = url, json=body_data)
                results = decode_bcast_url_response(url_reposnse)
                query_result = results['fin_query']

                if 'Error Occured' in query_result:
                    st.info(query_result)
                    numbers = -1
                else:
                    numbers = None
                    if results['phone_count'] is not None: numbers = sum([int(i) for i in results['phone_count']])

                inner_dict['seen'] =numbers
                inner_dict['seen_ref_num'] = blb_ref_num
                outer_dict[curr_iter] = inner_dict

            # Query for Eligible Count    
            first_run=True
            for curr_iter in range(iter_count):
                current_game_plan_dict = active_game_df.T.to_dict()[curr_iter]
                app_title = current_game_plan_dict['app_title']
                device_os = current_game_plan_dict['device_os']
                priority = current_game_plan_dict['priority']
                VERSION = str(datetime.datetime.now()).replace(':','').replace('-','').replace('.','').replace(' ','')
                blb_ref_num = query_tag+'_'+str(VERSION)

                inner_dict = {}
                outer_dict_df =  pd.DataFrame(outer_dict).T
                outer_dict_df=outer_dict_df.reset_index()

                if first_run:
                    outer_dict_df = outer_dict_df.merge(active_game_df.reset_index()[['index','priority']], how = 'left', on = 'index')
                    outer_dict_df['most_likely_ref']=''
                    outer_dict_df['not_most_likely_ref']=''
                    first_run=False
                    outer_dict_df=outer_dict_df.rename(columns = {'priority_x':'priority'})
                
                # Querying Eligible under Most Likely True
                most_like_flag = True
                seen_ref_num = list(outer_dict_df[outer_dict_df['index']==curr_iter]['seen_ref_num'])[0]
                high_p_ml_ref_num = list(outer_dict_df[outer_dict_df['priority']<priority].dropna(subset=['most_likely_ref'])['most_likely_ref'])
                fin_high_p_ml_excl_ref = [seen_ref_num]+high_p_ml_ref_num
                fin_high_p_ml_excl_ref = ','.join(fin_high_p_ml_excl_ref)

                query_tree = {}
                query_tree = RuleTree(base_tree=query_tree, field='installed_app', operator='!=',value=app_title).build_tree_all_and_rules()
                query_tree = RuleTree(base_tree=query_tree, field='install_likelihood', operator='=',value=most_like_flag).build_tree_all_and_rules()
                query_tree = RuleTree(base_tree=query_tree, field='exclude_phone_list_from_older_query', operator='=',value=True).build_tree_all_and_rules()
                if device_os.lower()!='all':
                    query_tree = RuleTree(base_tree=query_tree, field='device_os', operator='=',value=device_os).build_tree_all_and_rules()
                body_data = {'VERSION':VERSION, 
                                    'query_tag':query_tag,
                                    'st_query': '',
                                    'user':st.session_state.persistent_username,
                                    'compid':compid,
                                    'OP_SQL_CODE':0, 
                                    'OP_PHONE_COUNT':1,
                                    'OP_CSV_FILE':1,
                                    'share_field_dict':share_field_dict, 'tree':query_tree,
                                    'file_limit_number':0,'file_split_number':1,
                                    'exclude_phone_list_from_older_query':fin_high_p_ml_excl_ref,
                                    'add_phone_list_from_older_query':None,
                                    'intersect_phone_list_from_older_query':None,
                                    }
                if DEBUG: logging_function(log_level='WARNING',log_message=query_tree)
                url_reposnse = requests.post(url = url, json=body_data)
                url_reposnse = requests.post(url = url, json=body_data)
                results = decode_bcast_url_response(url_reposnse)
                query_result = results['fin_query']

                if 'Error Occured' in query_result:
                    st.info(query_result)
                    numbers = -1
                else:
                    numbers = None
                    if results['phone_count'] is not None: numbers = sum([int(i) for i in results['phone_count']])
                outer_dict[curr_iter]['device_os'] =device_os
                outer_dict[curr_iter]['priority'] =priority
                outer_dict[curr_iter]['most_likely_eligible'] =numbers
                outer_dict[curr_iter]['most_likely_ref'] =blb_ref_num
                outer_dict[curr_iter]['fin_high_p_ml_excl_ref'] = fin_high_p_ml_excl_ref

                # Querying Eligible under Most Likely False
                VERSION = str(datetime.datetime.now()).replace(':','').replace('-','').replace('.','').replace(' ','')
                blb_ref_num = query_tag+'_'+str(VERSION)
                most_like_flag = False
                high_p_nml_ref_num = list(outer_dict_df[outer_dict_df['priority']<priority].dropna(subset=['not_most_likely_ref'])['not_most_likely_ref'])
                fin_high_p_nonml_excl_ref = [seen_ref_num]+high_p_nml_ref_num
                fin_high_p_nonml_excl_ref = ','.join(fin_high_p_nonml_excl_ref)
                
                query_tree = {}
                query_tree = RuleTree(base_tree=query_tree, field='installed_app', operator='!=',value=app_title).build_tree_all_and_rules()
                query_tree = RuleTree(base_tree=query_tree, field='install_likelihood', operator='=',value=most_like_flag).build_tree_all_and_rules()
                query_tree = RuleTree(base_tree=query_tree, field='exclude_phone_list_from_older_query', operator='=',value=True).build_tree_all_and_rules()
                if device_os.lower()!='all':
                    query_tree = RuleTree(base_tree=query_tree, field='device_os', operator='=',value=device_os).build_tree_all_and_rules()

                body_data = {'VERSION':VERSION, 
                                    'query_tag':query_tag,
                                    'st_query': '',
                                    'user':st.session_state.persistent_username,
                                    'compid':compid,
                                    'OP_SQL_CODE':0, 
                                    'OP_PHONE_COUNT':1,
                                    'OP_CSV_FILE':1,
                                    'share_field_dict':share_field_dict, 'tree':query_tree,
                                    'file_limit_number':0,'file_split_number':1,
                                    'exclude_phone_list_from_older_query':fin_high_p_nonml_excl_ref,
                                    'add_phone_list_from_older_query':None,
                                    'intersect_phone_list_from_older_query':None,
                                    }
                if DEBUG: logging_function(log_level='WARNING',log_message=query_tree)
                url_reposnse = requests.post(url = url, json=body_data)
                url_reposnse = requests.post(url = url, json=body_data)
                results = decode_bcast_url_response(url_reposnse)
                query_result = results['fin_query']

                if 'Error Occured' in query_result:
                    st.info(query_result)
                    numbers = -1
                else:
                    numbers = None
                    if results['phone_count'] is not None: numbers = sum([int(i) for i in results['phone_count']])
                outer_dict[curr_iter]['not_most_likely_eligible'] =numbers
                outer_dict[curr_iter]['not_most_likely_ref'] =query_tag+'_'+str(VERSION)
                outer_dict[curr_iter]['fin_high_p_nonml_excl_ref'] = fin_high_p_nonml_excl_ref
        fin_df = pd.DataFrame(outer_dict).T
        fin_df = fin_df.reset_index()#.rename(columns={'index':'app_title'})
        if fin_df.shape[0]>0:
            fin_df = fin_df.merge(active_game_df.reset_index()[['index','app_title','estimated_install_rate','cpi']], how = 'left', on= 'index')
            
            fin_df['total_impressions'] = fin_df['most_likely_eligible']+fin_df['not_most_likely_eligible']
            fin_df['total_install'] = fin_df['estimated_install_rate'].astype(float)*fin_df['total_impressions']/100

            fin_df['most_likely_num_installs'] = 0.8*fin_df['total_install']
            fin_df['most_likely_num_installs']=fin_df['most_likely_num_installs'].astype('int64')
            fin_df['not_most_likely_num_installs'] = 0.2*fin_df['total_install']
            fin_df['not_most_likely_num_installs']=fin_df['not_most_likely_num_installs'].astype('int64')
            fin_df['total_install'] = fin_df['total_install'].astype('int64')
            
            fin_df['revenue_most_likely_bucket'] = fin_df['most_likely_num_installs']*fin_df['cpi']
            fin_df['revenue_not_most_likely_bucket'] = fin_df['not_most_likely_num_installs']*fin_df['cpi']
            fin_df['total_revenue'] = fin_df['revenue_most_likely_bucket']+fin_df['revenue_not_most_likely_bucket']
            fin_df['mms_profit_most_likely_bucket'] = fin_df['revenue_most_likely_bucket']-0.016*fin_df['most_likely_eligible']
            fin_df['mms_profit_not_most_likely_bucket'] = fin_df['revenue_not_most_likely_bucket']-0.016*fin_df['not_most_likely_eligible']
            for ref_col in ['seen_ref_num',  'most_likely_ref', 'fin_high_p_ml_excl_ref',
                'not_most_likely_ref', 'fin_high_p_nonml_excl_ref']:
                fin_df[ref_col] = fin_df[ref_col].apply(lambda x: 'BLB App Ref# '+x)
            fin_df = fin_df[['app_title','priority', 'device_os','estimated_install_rate','cpi','seen','total_impressions','most_likely_eligible', 'not_most_likely_eligible','most_likely_num_installs','not_most_likely_num_installs','total_install','revenue_most_likely_bucket','revenue_not_most_likely_bucket','total_revenue','mms_profit_most_likely_bucket','mms_profit_not_most_likely_bucket', 'seen_ref_num',  
                'most_likely_ref', 'fin_high_p_ml_excl_ref',
                'not_most_likely_ref', 'fin_high_p_nonml_excl_ref']]
        st.dataframe(fin_df,column_config={'cpi':st.column_config.NumberColumn(
                    "cpi",
                    min_value=0,
                    format="$ %.2f",
                    required=True),'revenue_most_likely_bucket':st.column_config.NumberColumn(
                    "revenue_most_likely_bucket",
                    min_value=0,
                    format="$ %.2f",
                    required=True),'revenue_not_most_likely_bucket':st.column_config.NumberColumn(
                    "revenue_not_most_likely_bucket",
                    min_value=0,
                    format="$ %.2f",
                    required=True),'total_revenue':st.column_config.NumberColumn(
                    "total_revenue",
                    min_value=0,
                    format="$ %.2f",
                    required=True),'mms_profit_most_likely_bucket':st.column_config.NumberColumn(
                    "Profit on Most Likely (Revenue - MMS Cost)",
                    min_value=0,
                    format="$ %.2f",
                    required=True),'mms_profit_not_most_likely_bucket':st.column_config.NumberColumn(
                    "Profit on Not-Most Likely (Revenue - MMS Cost)",
                    min_value=0,
                    format="$ %.2f",
                    required=True),'estimated_install_rate':st.column_config.NumberColumn(
                    "estimated_install_rate",
                    min_value=0,
                    format="%.2f%%",
                    required=True)})
        dnrp_ph.markdown(f'<p style="text-align: left; box-sizing: border-box;color:blue;">Results:</p>', unsafe_allow_html=True)
        st.session_state.fetch_clicked = False
    else:
        st.session_state.fetch_clicked = False
        st.info('Please commit vairables!')

