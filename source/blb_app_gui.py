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
import traceback
import os
import io
from io import BytesIO
import time
import json
from blb_functions import logger,check_phone_num_length, logging_function, decode_bcast_url_response,dep_environment,APP_VERSION,DEBUG,RuleTree, new_model_compid_list

st.set_page_config(layout="wide", page_title=f'BLB.{APP_VERSION}', 
                   page_icon='dependencies/mobi_logo.png',
                   )
#st.logo('dependencies/mobi_logo.png',)
with open('config_blb_app.json') as json_file:
    app_config = json.load(json_file)

DEBUG_FLAG = False
IS_PROD = True

PORT_NUM = app_config['backend_port']
backend_base_url =app_config['backend_url']

def preprocess_upload_files(uploaded_file, list_type = ''):
  if uploaded_file is not None:
    if 'zip' in str(uploaded_file.type):
        zdf_list = []
        with zipfile.ZipFile(uploaded_file) as uzf:
            for zfname in uzf.namelist():
                if ('.csv' in zfname) and (not ('/.' in zfname)):
                    temp_df = pd.read_csv(uzf.open(zfname))
                    zdf_list.append(temp_df)
        zdf = pd.concat(zdf_list)
        col_names = list(zdf.columns)
        zdf = zdf[[col_names[0]]]
        if list_type=='phone':
          zdf.columns = ['phone']
          zdf['check_len'] = check_phone_num_length(zdf['phone'], return_series = True)
          zdf = zdf[zdf['check_len']==10]
          zdf = zdf[['phone']]
        else:
          zdf.columns = ['zipcode']
          zdf['check_len'] = check_phone_num_length(zdf['zipcode'], return_series = True)
          zdf = zdf[zdf['check_len']==5]
          zdf = zdf[['zipcode']]
    else:
        zdf = pd.read_csv(uploaded_file)
        col_names = list(zdf.columns)
        zdf = zdf[[col_names[0]]]
        if list_type=='phone':
          zdf.columns = ['phone']
          zdf['check_len'] = check_phone_num_length(zdf['phone'], return_series = True)
          zdf = zdf[zdf['check_len']==10]
          zdf = zdf[['phone']]
        else:
          zdf.columns = ['zipcode']
          zdf['check_len'] = check_phone_num_length(zdf['zipcode'], return_series = True)
          zdf = zdf[zdf['check_len']==5]
          zdf = zdf[['zipcode']]
    zdf = zdf.reset_index(drop=True)
  return zdf

# def check_password():
#   """Returns `True` if the user had the correct password."""

#   def password_entered():
#     """Checks whether a password entered by the user is correct."""
#     if hmac.compare_digest(st.session_state["password"], st.secrets["password"]):
#         st.session_state["password_correct"] = True
#         del st.session_state["password"]  # Don't store the password.
#     else:
#         st.session_state["password_correct"] = False

#   # Return True if the password is validated.
#   if st.session_state.get("password_correct", False):
#     return True

#   # Show input for password.
#   st.text_input("Password", type="password", on_change=password_entered, key="password")
#   if "password_correct" in st.session_state:
#     st.error("😕 Password incorrect")
#   return False


# if not check_password():
#   st.stop()  # Do not continue if check_password is not True.

# # Main Streamlit app starts here

filter_include_zipcode = None
filter_exclude_zipcode = None 
filter_add_phone_list = None 
filter_intersect_phone_list = None
filter_remove_phone_list = None
filter_add_test_phone_list = None
exclude_phone_list_from_older_query = None
add_phone_list_from_older_query = None
intersect_phone_list_from_older_query = None
if "submit_button_diasble" not in st.session_state:
   st.session_state.submit_button_diasble=False
def disable_on_click ():
   st.session_state.submit_button_diasble = True

def upload_files(VERSION,filter_include_zipcode = None, 
                         filter_exclude_zipcode = None, 
                         filter_add_phone_list = None, filter_add_test_phone_list=None,
                         filter_intersect_phone_list = None, 
                         filter_remove_phone_list = None, compid=0,backend_base_url='', PORT_NUM=app_config['backend_port'], query_tag = ''):
            if filter_include_zipcode is not None:
                filter_include_zipcode = preprocess_upload_files(filter_include_zipcode, list_type = 'zip')
                stringio  = io.StringIO()
                filter_include_zipcode.to_csv(stringio, index=False)
                #('came here')
                resp = requests.post(url=f'{backend_base_url}:{PORT_NUM}/filter_include_zipcode/?compid={compid}&query_tag={query_tag}&version={VERSION}', files={'file':stringio.getvalue()}) 
                print(resp)
            if filter_exclude_zipcode is not None:
                filter_exclude_zipcode = preprocess_upload_files(filter_exclude_zipcode, list_type = 'zip')
                stringio  = io.StringIO()
                filter_exclude_zipcode.to_csv(stringio, index=False)
                resp = requests.post(url=f'{backend_base_url}:{PORT_NUM}/filter_exclude_zipcode/?compid={compid}&query_tag={query_tag}&version={VERSION}', files={'file':stringio.getvalue()}) 
                print(resp)
            if filter_add_phone_list is not None:
                filter_add_phone_list = preprocess_upload_files(filter_add_phone_list, list_type = 'phone')
                stringio  = io.StringIO()
                filter_add_phone_list.to_csv(stringio, index=False)
                #comp_ver = 'comp'+str(compid)+'__'+str(VERSION)
                #print('calling add phone')
                #print(comp_ver)
                print(filter_add_phone_list.head(2))
                resp = requests.post(url=f'{backend_base_url}:{PORT_NUM}/filter_add_phone_list/?compid={compid}&query_tag={query_tag}&version={VERSION}', files={'file':stringio.getvalue()}) 
                print(resp) 
                #print('called add phone')
                
            if filter_add_test_phone_list is not None:
                filter_add_test_phone_list = preprocess_upload_files(filter_add_test_phone_list, list_type = 'phone')
                stringio  = io.StringIO()
                filter_add_test_phone_list.to_csv(stringio, index=False)
                resp = requests.post(url=f'{backend_base_url}:{PORT_NUM}/filter_add_test_phone_list/?compid={compid}&query_tag={query_tag}&version={VERSION}', files={'file':stringio.getvalue()}) 
                print(resp) 
            if filter_intersect_phone_list is not None:
                filter_intersect_phone_list = preprocess_upload_files(filter_intersect_phone_list, list_type = 'phone')
                stringio  = io.StringIO()
                filter_intersect_phone_list.to_csv(stringio, index=False)
                resp = requests.post(url=f'{backend_base_url}:{PORT_NUM}/filter_intersect_phone_list/?compid={compid}&query_tag={query_tag}&version={VERSION}', files={'file':stringio.getvalue()}) 
                print(resp)
            if filter_remove_phone_list is not None:
                filter_remove_phone_list = preprocess_upload_files(filter_remove_phone_list, list_type = 'phone')
                stringio  = io.StringIO()
                filter_remove_phone_list.to_csv(stringio, index=False)
                resp = requests.post(url=f'{backend_base_url}:{PORT_NUM}/filter_remove_phone_list/?compid={compid}&query_tag={query_tag}&version={VERSION}', files={'file':stringio.getvalue()}) 
                print(resp)

if not DEBUG_FLAG:
    def check_password():
        """Returns `True` if the user had a correct password."""

        def login_form():
            """Form with widgets to collect user information"""
            
            with st.form("Credentials"):
                
                st.markdown('<h2 style="text-align: center;font-family:Tahoma; color:white;background-color: #8dc63f">BLB</h2>', unsafe_allow_html=True)
                st.markdown('<h4 style="text-align: center;font-family:Tahoma; color:white;background-color: #8dc63f">(Broadcast List Builder)</h4>', unsafe_allow_html=True)
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
                logging_function(log_level='WARNING',log_message=f'{st.session_state.persistent_username} login detected in BLB')
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
            logging_function(log_level='ERROR',log_message=f'{st.session_state.persistent_username} entered wrong password')
            st.error("😕 User not known or password incorrect")
        return False


    if not check_password():
        st.stop()

    # Main Streamlit app starts here



###streamlit run qp_v2.py


def get_list_key_val(df):
    key_pair_list = []
    temp = df.T.to_dict()
    for k in temp.keys():
        key_pair_list.append(temp[k])
    return key_pair_list

grm_df = pd.DataFrame()
grm_df['title'] = ['A','B','C','D','E']
grm_df['value'] = [1,2,3,4,5]
grm_key_val = get_list_key_val(grm_df)

try:
  df_comp = pd.read_csv('dependencies/company_names.csv')
  df_comp = df_comp.dropna()
  comp_key_val = get_list_key_val(df_comp)

  df = pd.read_csv('dependencies/state_names.csv')
  df = df.dropna()
  states_list = list(df['state_name'])

  df = pd.read_csv('dependencies/app_titles.csv')
  df = df.dropna()
  app_title_list = list(df['app_title'])

  df_bu = pd.read_csv('dependencies/bu_names.csv')
  df_bu = df_bu.dropna()
  bu_name_list = list(df_bu['bu_name'])
  comp_bu_list=[]

  df_optin = pd.read_csv('dependencies/opt_in_source.csv')
  df_optin = df_optin.dropna()
  opt_in_source_list = list(df_optin['opt_in_source'])
  comp_opt_in_source_list=[]
except Exception as e:
  error_str = traceback.format_exception_only(e)
  error_str = ' '.join(error_str).replace('\n',' ')
  logging_function(log_level='ERROR',log_message=error_str)


#df_zip = pd.read_csv('dependencies/zipcodes.csv')
#df_zip = df_zip.dropna()
#zipcodes_list = list(df_zip['zipcode'])
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
st.html('<h2 style="text-align: center;font-family:Tahoma; color:white;background-color: #8dc63f;border-radius: 10px;position:relative;left:0px;right:0px;">Broadcast List Builder</h2>')
#col1, col2 = st.columns(2)


if not IS_PROD:
  #mode_options = ['Create a new Query']
  mode_options =  ['Create a new Query','Reload a previous Query (20-digit Reference Number required)']
  mode_of_query = st.radio(label= "Select the Mode of Query:", options=mode_options, index=0)
  if not "proceed_button" in st.session_state:
      st.session_state.proceed_button = False
  proceed_button = True #st.button('Proceed',type='primary')
  if proceed_button:
    st.session_state.proceed_button = True
  #st.markdown('<h4 style="text-align: left;font-family:Tahoma; color:black;">Start Here!</h4>', unsafe_allow_html=True)

  if 'Reload a previous Query' in mode_of_query:
    older_version = st.text_input(label='Enter the 20-digit Reference Number of previous query:', max_chars=20)
    st.info(older_version)

  if mode_of_query=='Create a new Query':
    st.markdown('<p style="text-align: left;font-family:Tahoma; color:blue;">Start Here!</p>', unsafe_allow_html=True)

def stop_streamlit():
   st.stop()

query_tag = ''
st.markdown(f'<p style="text-align: right;font-family:Tahoma; font-size:12px;color:black; position: relative; top:10px;"></p>', unsafe_allow_html=True)
query_tag = st.text_input(label='Enter a Friendly name for your query:',key = 'query_tag',placeholder='e.g. flight_id OR brand_xx_game_xx_bcastdate_xx',)
print(query_tag)
if (query_tag is None) or (query_tag==''):
  st.info('Please provide a name for query!')
  st.cache_data.clear()
  comp_name_key = None
  
else:
  query_tag = query_tag.replace('/','_')
  comp_name_key = st.selectbox("Brand Name", options=list(df_comp['company_names']), index=None,placeholder='Select from dropdown',)
if (comp_name_key is not None):
    
    compid = int(df_comp[df_comp['company_names']==comp_name_key]['comp_id'].unique())
    comp_bu_list = list(df_bu[df_bu['company_id']==compid]['bu_name'])
    comp_opt_in_source_list = list(df_optin[df_optin['company_id']==compid]['opt_in_source'])
    fields_dict  =   {

          'install_likelihood': {
            'label': 'Most Likely to Install','tooltip':"""This filter is used to select users who are currently active and are highly likely to install something. Switch Turned Off: If the switch is off, the filter will give you users who are not likely to install. Switch Turned On: If the switch is on, the filter will give you users who are most likely to install.""",
            #Use this filter to get active users which are Most Likely to Install. If the switch is turned-off then it means youll get Not-Likely to install users, turn-on means you get Most-Likely to install users,
            'type': 'boolean','canCompareFieldWithField': 'false',
            'widgets': {
               'boolean': {
                  'operators': ['equal']
                  }
                  }
                },
          'install_probability': {
            'label': 'Install Probability',
            'type': 'number',
            'valueSources': ["value"],'tooltip':"""Install Probability ranges from 0 to 1. Users are categorized as "Most Likely to Install" if their probability is 0.5 or higher. When filtering "Between values x and y", it includes values where x is included and y is not.""",
            'fieldSettings': {
              'min':0, 'max':1,'step':0.01,}
            },
          'device_os': {
              'label': 'Device OS','tooltip':"""Choose one or more device operating systems from the list. Users whose operating system isn't recognized will be categorized as "Unknown".""",
              'type': 'select',
              'fieldSettings': {'listValues': ['iOS','Android','Unknown']},
            },
          'installed_app':{
              'label':'Installed App/Game','tooltip':"""By selecting one or more games here, you will get users who are active and have installed the specified game(s) through Mobivity flights.""",
              'type':'select',
              'valueSources': ["value"],
            'fieldSettings': {
              'listValues': app_title_list,'showSearch':False}
            },
          'installed_with_site_id': {
            'label': 'Installed through a specific Site ID. Type-in Site ID','tooltip':"""Enter a site ID here to get users who are currently active and have installed something through the specified site ID.""",
            'type': 'text'
            },
          'seen_app':{
              'label':'Seen App/Game','tooltip':"""By selecting one or more games here, you will get users who have already received the broadcast for the mentioned game(s).""",
              'type':'select',
              'valueSources': ["value"],
            'fieldSettings': {
              'listValues': app_title_list}
            },
            'broadcast_date':{'label':'Broadcast Date', 'tooltip':"""You can refer to users who received a broadcast on a selected date or within a date range. If you also use the Seen App/Game filter, you can specifically refer to users who received the broadcast for that game during the chosen timeframe. Operator BETWEEN doesn't include the upper range limit.""",
                              'type':'date','valueSources': ["value"],'fieldSettings':{'dateFormat':'yyyy-MM-DD','valuePlaceholder':'2010-01-31',#'timeFormat':'HH:mm:ss'
              },
            },
            'last_seen_date':{'label':'Last Seen Date','tooltip':"""You can mention a date here which will give you active users if they got their last broadcast on mentioned date. If you also use the Seen App/Game filter, you can specifically refer to users who received the latest broadcast for that game during the chosen timeframe. Operator BETWEEN doesn't include the upper range limit.""",
                               'type':'date','valueSources': ["value"],'fieldSettings':{'dateFormat':'yyyy-MM-DD','valuePlaceholder':'2010-01-31',#'timeFormat':'HH:mm:ss'
                },
              },
          'clicked_app':{
              'label':'Clicked App/Game','tooltip':'By selecting one or more games here, you will get active users who have clicked on the broadcast for the mentioned game(s)',
              'type':'select',
              
              'valueSources': ["value"],
            'fieldSettings': {
              'listValues': app_title_list}
            },
          'state_name': {
            'label': 'State','tooltip':'By selecting one or more states here, you will get active users who reside in those states based on their zip codes.',
            'type': 'select',
            
            'valueSources': ["value"],
            'fieldSettings': {
              'listValues': states_list}
            },
          'bu_name': {
            'label': 'BU Name', 'tooltip':'By selecting one or more BU Names here, you will get active users who belong to those defined Business Units (BUs).',
            'type': 'select',
            
            'valueSources': ["value"],
            'fieldSettings': {
              'listValues': comp_bu_list}
            },
            'opt_in_source': {
            'label': 'Top Level Opt-in Source Keyword', 'tooltip':'By selecting one or more Opt-in Sources here, you will see active users who have opted in via the mentioned source.',
            'type': 'select',
            
            'valueSources': ["value"],
            'fieldSettings': {
              'listValues': comp_opt_in_source_list}
            },
          'camp_keyword': {
            'label': 'Campaign Keyword', 'tooltip':'You can enter only 1 Campaign Keyword. You can either type the complete keyword name or use string pattern matching by selecting operators like "Contains", "Starts with", etc. The results will get you all active users associated with keywords that match your search.',
            'type': 'text'
            },

          'promo_tag_clicked': {
            'label': 'Clicked on a Promo. Type-in Promo Tag','tooltip':'You can enter only 1 Promo Tag. You can either type the complete tag or use string pattern matching by selecting operators like "Contains", "Starts with", etc. The results will get you all active users who clicked on promos matching the tag you entered.',
            'type': 'text'
            },
            'promo_name_clicked': {
            'label': 'Clicked on a Promo. Type-in Promo ID', 'tooltip':'You can enter only 1 Promo ID. You can either type the complete ID or use string pattern matching by selecting operators like "Contains", "Starts with", etc. The results will show you all active users who clicked on promos matching the Promo ID you entered.',
            'type': 'text'
            },
            # 'grm_bucket': {
            # 'label': 'GRM Bucket', 'tooltip':'Select one or more GRM bucket to get active users under those bucket(s)',
            # 'type': 'select',
            # 'valueSources': ["value"],
            # 'fieldSettings': {
            #   'listValues': grm_key_val}
            # },
            'redeemer_flag': {
            'label': 'Redeemer / Non-Redeemer', 'tooltip':'Select "Redeemer" to get active users who have redeemed at least once through Mobivity. Select "Non-Redeemer" to see users who have never redeemed.',
            'type': 'select',
            'valueSources': ["value"],
            'fieldSettings': {
              'listValues': ['Redeemer','Non-Redeemer']}
            },
            'redeemer_time_of_day': {
            'label': 'Redeem Time of Day','tooltip':'Choose a time of day to see active users who redeemed at that specific time.',
            'type': 'select',
            'valueSources': ["value"],
            'fieldSettings': {
              'listValues': ['Breakfast','Dinner','Lunch','LateEveningSnack']}
            },
            'redeemer_day_of_week': {
            'label': 'Redeem Day of Week', 'tooltip':'Choose a day of the week to see active users who redeem on that specific day of week.',
            'type': 'select',
            'valueSources': ["value"],
            'fieldSettings': {
              'listValues': ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']}
            },
            'days_since_last_redemption': {
            'label': 'Days Since Last Redemption', 'tooltip':'This filter will get you the active users who latest redeemed within certain timeframe in number of days from today. Enter a number (in days) to get users falling in specified bucket of Last Redempetion timeframe.',
            'type': 'number',
            'valueSources': ["value"],
            'fieldSettings': {
              'min':0}
            },
            'engaged': {
            'label': 'Engaged (clicked on a link)', 'tooltip':'This filter will get you the active users who have clicked on any broadcast. Not Engaged means, never clicked. Engaged means clicked atleast once.',
            'type': 'select',
            'valueSources': ["value"],
            'fieldSettings': {'listValues': ['Engaged','Not Engaged']}
            },
            'tenure': {
            'label': 'Tenure in Days', 'tooltip':'This filter will get you the active users whose tenure with Mobivity falls within certain timeframe in number of days utill today. Enter a number (in days) to get users falling in specified bucket of Tenrue.',
            'type': 'number',
            'valueSources': ["value"],
            'fieldSettings': {
              'min':0}
            },
          # 'add_phone_list_from_older_query': {'label':"Add phone list from old BLB App Query. Enter BLB App reference numbers", 
          #                                 'type':'text', 'fieldSettings': {'valuePlaceholder':'Ref#1,Ref#2,Ref#3'}},
          # 'interset_phone_list_from_older_query': {'label':"Intersect phone list from old BLB App Query. Enter BLB App reference numbers", 
          #                                 'type':'textarea', 'fieldSettings': {'valuePlaceholder':'Ref#1,Ref#2,Ref#3'}},                            

        }
    if (not compid in new_model_compid_list) and ('install_probability' in fields_dict.keys()):
       fields_dict.pop('install_probability')
    share_field_dict = {}
    for fdk in fields_dict.keys():
      share_field_dict[fdk]=''

    url = f"{backend_base_url}:{PORT_NUM}/st_query/"
    config = {'fields':fields_dict,'setting':{'canCompareFieldWithField': False}}
    submit_button=None
    #if st.session_state.show_main_tree:
    st.markdown('<h4 style="text-align: left; box-sizing: border-box;color:black;">Main Filter Criteria:</h4>', unsafe_allow_html=True)
    with st.container(border=True):
      #filter_tree_col1, filter_tree_col2 = st.columns(2)
      st.markdown('<h4 style="text-align: right; box-sizing: border-box;color:black;">Add More Rule or Groups</h4>', unsafe_allow_html=True)
      return_val = condition_tree(
        config,
        return_type='sql',
        placeholder='',key='main_tree',min_height=600 , 
      )
    need_zipcode=False
    if True:
      OP_PHONE_COUNT=0
      OP_SQL_CODE=0 
      OP_CSV_FILE=0
      if return_val is None:
         return_val = ''
      st.markdown('<h4 style="text-align: left; box-sizing: border-box;color:black;">Additional Filter Criteria:</h4>', unsafe_allow_html=True)
      with st.container(border=True): #-- Select Zipcodes
            if st.checkbox(value=False, label="Zipcodes: Filter to select Zipcodes (File upload required)"):
                
                st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
                zs_radio = st.radio(label=' ', options = ['AND','OR'], index=0, horizontal=True,key='rzu1')
                filter_include_zipcode = st.file_uploader("Upload a list of 5 digit zipcodes, only 1 column named zipcode; only CSV File or a ZIP file having only CSV files (not a zip of folders)", type=['.csv','.zip'],key='filter_include_zipcode')
                return_val = f"{return_val} {zs_radio} filter_include_zipcode = true"
                

      with st.container(border=True): #-- Exclude Zipcodes
            if st.checkbox(value=False, label="Zipcodes: Filter to exclude Zipcodes (File upload required)"):
                st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
                ze_radio = st.radio(label=' ', options = ['AND','OR'], index=0, horizontal=True,key='rzu2')
                filter_exclude_zipcode = st.file_uploader("Upload a list of 5 digit zipcodes, only 1 column named zipcode; only CSV File or a ZIP file having only CSV files (not a zip of folders)", type=['.csv','.zip'],key='filter_exclude_zipcode')
                return_val = f"{return_val} {ze_radio} filter_exclude_zipcode = true"

      with st.container(border=True): #-- Add phones
            if st.checkbox(value=False, label="Phone: Add A List of User's Phones (File upload required)"):
                st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
                padd_radio= st.radio(label=' ', options = ['AND'], index=0, horizontal=True,key='rfu1')
                filter_add_phone_list = st.file_uploader("Upload a list of 10 digit phone numbers without special characters like +,- etc; only 1 column named phone; only CSV File or a ZIP file having only CSV files (not a zip of folders)", type=['.csv','.zip'],key='filter_add_phone_list')
                return_val = f"{return_val} {padd_radio} filter_add_phone_list = true"

      with st.container(border=True):#-- Add Test Phones
            if st.checkbox(value=False, label="Phone: Add A List of Test Phones (File upload required)"):
                st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
                paddt_radio = st.radio(label=' ', options = ['AND'], index=0, horizontal=True,key='rfu2')
                st.info('These phones will be present in all the output files')
                filter_add_test_phone_list = st.file_uploader("Upload a list of 10 digit phone numbers without special characters like +,- etc; only 1 column named phone; only CSV File or a ZIP file having only CSV files (not a zip of folders)", type=['.csv','.zip'],key='filter_add_test_phone_list')
                return_val = f"{return_val} {paddt_radio} filter_add_test_phone_list = true"

      with st.container(border=True): #-- Intersect Phones
            if st.checkbox(value=False, label="Phone: Intersect Phone with a List of User's Phone (File upload required)"):
                st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
                pis_radio = st.radio(label=' ', options = ['AND'], index=0, horizontal=True,key='rfu3')
                filter_intersect_phone_list = st.file_uploader("Upload a list of 10 digit phone numbers without special characters like +,- etc; only 1 column named phone; only CSV File or a ZIP file having only CSV files (not a zip of folders)", type=['.csv','.zip'],key='filter_intersect_phone_list')
                return_val = f"{return_val} {pis_radio} filter_intersect_phone_list = true"

      with st.container(border=True): #-- Exclude Phones
            if st.checkbox(value=False, label="Phone: Exclude A List of Phone (File upload required)."):
                st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
                prm_radio = st.radio(label=' ', options = ['AND'], index=0, horizontal=True,key='rfu4')
                filter_remove_phone_list = st.file_uploader("Upload a list of 10 digit phone numbers without special characters like +,- etc; only 1 column named phone; only CSV File or a ZIP file having only CSV files (not a zip of folders)", type=['.csv','.zip'],key='filter_remove_phone_list')
                return_val = f"{return_val} {prm_radio} filter_remove_phone_list = true"

      with st.container(border=True): #-- Add phone from BLB Ref
         
         if st.checkbox(value=False, label="Old BLB Ref: Add phone list from old BLB App Query. Enter BLB App reference numbers"):
            st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
            st.radio(label='', options = ['AND'], index=0, horizontal=True,key='rbr1')
            add_phone_list_from_older_query = st.text_input(label='Enter old BLB App reference number to Add. If more than 1 then separate by comma:', key = 'add_phone_list_from_older_query')

      with st.container(border=True): #-- Exclude phone from BLB Ref
         if st.checkbox(value=False, label="Old BLB Ref: Exclude phone list from old BLB App Query. Enter BLB App reference numbers"):
            st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
            st.radio(label=' ', options = ['AND'], index=0, horizontal=True,key='rbr2')
            exclude_phone_list_from_older_query = st.text_input(label='Enter old BLB App reference number To Exclude. If more than 1 then separate by comma:', key = 'exclude_phone_list_from_older_query')

      with st.container(border=True): #-- Intersect phone form BLB Ref
         if st.checkbox(value=False, label="Old BLB Ref: Intersect with phone list from old BLB App Query. Enter BLB App reference numbers"):
            st.markdown(f'<p style="text-align: left; box-sizing: border-box;">Condition with above <span style="color:blue;">Main Filter Criteria</span>:</p>', unsafe_allow_html=True)
            st.radio(label=' ', options = ['AND'], index=0, horizontal=True,key='rbr3')
            intersect_phone_list_from_older_query = st.text_input(label='Enter old BLB App reference number To Intersect. If more than 1 then separate by comma:', key = 'intersect_phone_list_from_older_query')

      
      st.markdown(f'<hr width=100%>', unsafe_allow_html=True)
      
      file_limit_split_col1, file_limit_split_col2 = st.columns(2)
      with file_limit_split_col1:
          st.markdown(f'<p style="text-align: left; box-sizing: border-box;color:blue;">Do you want to limit number of    Phones in output file?</p>', unsafe_allow_html=True)
          file_limit = st.radio(label=" ",options=['Yes','No'], index=1,key='phone_limit')
          if file_limit=='Yes':
            with file_limit_split_col2:
               st.markdown(f'<p style="text-align: left; box-sizing: border-box;color:blue;">How many Phone numbers do you want in output file?</p>', unsafe_allow_html=True)
               file_limit_number = st.number_input(label=' ', min_value=1, max_value=50000000, value="min", step=1,label_visibility="visible")
          else:
            file_limit_number=0
      st.markdown(f'<hr width=90%>', unsafe_allow_html=True)
      file_split_col1, file_split_col2 = st.columns(2)
      with file_split_col1:
          st.markdown(f'<p style="text-align: left; box-sizing: border-box;color:blue;">Do you want to break output Phone numbers in multiple lists of equal size?</p>', unsafe_allow_html=True,)
          file_split= st.radio(label=" ",options=['Yes','No'], index=1, key='file_split')
          if file_split=='Yes':
            with file_split_col2:
                st.markdown(f'<p style="text-align: left; box-sizing: border-box;color:blue;">How many parts?</p>', unsafe_allow_html=True)
                file_split_number = st.number_input(label=' ', min_value=1, max_value=1000, value="min", step=1,label_visibility="visible")
          else:
             file_split_number=1


      st.markdown('<h4 style="text-align: Left; box-sizing: border-box;color:black;">Select Output Types:</h4>', unsafe_allow_html=True) 
      if st.checkbox(value=False, label='Count of Unique Phones'):
        OP_PHONE_COUNT = 1
      if st.checkbox(value=False, label='SQL Code'):
        OP_SQL_CODE = 1
      if st.checkbox(value=False, label='Generate list of Phones as CSV file'):
        OP_CSV_FILE = 1
    ### Submit Button
      submit_button = st.button('Submit', type='primary',key='main_submit', 
                                #on_click=disable_on_click, disabled=st.session_state.submit_button_diasble
                                )
      tree_config = st.session_state['main_tree']
      
    if submit_button:
      VERSION = str(datetime.datetime.now()).replace(':','').replace('-','').replace('.','').replace(' ','').replace('/','')
      if DEBUG_FLAG:
          st.code(return_val)
      if DEBUG_FLAG:
          st.code(return_val)
      if not DEBUG_FLAG:
        tree_config = st.session_state['main_tree']
        
        if not filter_include_zipcode is None:
            zs_tree = RuleTree(base_tree = {},field='filter_include_zipcode', operator='=', value=True).build_tree_all_and_rules( )
            if 'children' in tree_config.keys():
              if len(tree_config['children'])>0:
                tree_config = {'type': 'group','properties': {'conjunction': zs_radio, 'not': False},
                'children': [tree_config, zs_tree]}
              else:
                tree_config = zs_tree
            else:
                tree_config = zs_tree
        if not filter_exclude_zipcode is None:
            ze_tree = RuleTree(base_tree = {},field='filter_exclude_zipcode', operator='=', value=True).build_tree_all_and_rules( )
            if 'children' in tree_config.keys():
              if len(tree_config['children'])>0:
                tree_config = {'type': 'group','properties': {'conjunction': ze_radio, 'not': False},
                'children': [tree_config, ze_tree]}
              else:
                tree_config = ze_tree
            else:
                tree_config = ze_tree

        
        upload_done=False
        if (('filter_include_zipcode = true' in return_val) and filter_include_zipcode is None) or (('filter_exclude_zipcode = true' in return_val) and filter_exclude_zipcode is None) or (('filter_add_phone_list = true' in return_val) and filter_add_phone_list is None) or (('filter_add_test_phone_list = true' in return_val) and filter_add_test_phone_list is None) or (('filter_intersect_phone_list = true' in return_val) and filter_intersect_phone_list is None) or (('filter_remove_phone_list = true' in return_val) and filter_remove_phone_list is None) :
           st.error('Some files are not uploaded!')
        else:
          with st.spinner(text = "Uploading files..."):
            upload_files(VERSION,filter_include_zipcode, 
                              filter_exclude_zipcode, 
                              filter_add_phone_list, filter_add_test_phone_list,
                              filter_intersect_phone_list, 
                              filter_remove_phone_list, compid=compid,backend_base_url=backend_base_url, query_tag=query_tag)
            upload_done=True
            logging_function(log_level='INFO',log_message=f'upload completed')

          body_data = {'VERSION':VERSION, 
                      'query_tag':query_tag,
                      'compid':int(compid),
                      'st_query': return_val,
                      'user':st.session_state.persistent_username,
                      'OP_SQL_CODE':OP_SQL_CODE, 
                      'OP_PHONE_COUNT':OP_PHONE_COUNT,
                      'OP_CSV_FILE':OP_CSV_FILE,
                      'share_field_dict':share_field_dict, 'tree':tree_config,
                      'file_limit_number':file_limit_number,'file_split_number':file_split_number,
                      'exclude_phone_list_from_older_query':exclude_phone_list_from_older_query,
                      'add_phone_list_from_older_query':add_phone_list_from_older_query,
                      'intersect_phone_list_from_older_query':intersect_phone_list_from_older_query,
                      }
          logging_function(log_level='INFO',log_message=f'body prepared')
          ref_str = f"""--BLB App Reference Number: {query_tag}_{VERSION} --"""
          #st.info(body_data['tree'])
          
          if OP_PHONE_COUNT or OP_CSV_FILE or OP_SQL_CODE:
            st.info(ref_str)
            if upload_done:
              with st.spinner('Querying. Please Wait....'):
                print(upload_done,'upload_done')
                url_reposnse = requests.post(url = url, json=body_data)
                logging_function(log_level='INFO',log_message=f'request sent')
                results = decode_bcast_url_response(url_reposnse)
                logging_function(log_level='INFO',log_message=f'response recvd')
                #print(results)
                sql_str2 = results['fin_query']
                #print(type(url_reposnse.content))
                print(upload_done,'upload_done')
                st.markdown(f'<p style="text-align: left; box-sizing: border-box;color:blue;">Results:</p>', unsafe_allow_html=True)
                rest_str = sql_str2
                if '/*pc_starts' in rest_str:
                  phone_str_op, rest_str = rest_str.split('/*pc_starts')
                  phone_str_op, rest_str = rest_str.split('pc_ends*/')
                if '/*fn_starts' in rest_str:
                  fn_str_op, rest_str = rest_str.split('/*fn_starts')
                  fn_str_op, rest_str = rest_str.split('fn_ends*/')
                with st.container(border=True):
                  if OP_PHONE_COUNT+OP_CSV_FILE>0:
                    
                    phone_count_list = results['phone_count']
                    #print(phone_count_list)
                    phone_str_op = ', '.join(phone_count_list)
                    st.markdown(f'<p style="text-align: left; box-sizing: border-box;color:black;">Total Unique Phone Numbers are {phone_str_op}</p>', unsafe_allow_html=True)
                  if OP_CSV_FILE>0:
                    file_list = results['op_file_names']
                    fn_str_op = ' </br> '.join(file_list)
                    st.markdown(f'<p style="text-align: left; box-sizing: border-box;color:black;">Results of Above Query Available in</p>', unsafe_allow_html=True)
                    st.markdown(f'<p style="text-align: left; box-sizing: border-box;color:black;">{fn_str_op}</p>', unsafe_allow_html=True)
                    
                    df_list_for_download =  []
                    #st.write()
                    for i in range(len(file_list)):
                      dl_file = list(file_list)[i]
                      dl_df = results[dl_file].replace('"','').split('\n')
                      df = pd.DataFrame()
                      col_name = 'phone'
                      df[col_name] = dl_df
                      df = df[df[col_name]!='']
                      df_list_for_download.append(df)
                    zip_filename = f'{query_tag}_{VERSION}.zip'
                    zip_filename_full = f'output/{zip_filename}'
                    with zipfile.ZipFile(zip_filename_full, 'w') as zf:
                        i=0 #this iterator to make sure each .csv will have a different name
                        for df in df_list_for_download:
                            dl_fname = file_list[i]
                            dl_fname = dl_fname.split('/')[-1]
                            df = df[[col_name]]
                            df.to_csv(dl_fname, index=False) #this will convert the dataframe to a .csv
                            zf.write(dl_fname) #this will put the .csv in the zipfile
                            os.remove(dl_fname) #this will delete the .csv created 
                            i+=1
                    with open(zip_filename_full, "rb") as fp:
                        st.markdown('<p style="text-align: Left; box-sizing: border-box;color:blue;">Note: Click on below download button will erase all outputs so please note the required details before downloading</p>', unsafe_allow_html=True) 
                        btn = st.download_button(
                            label="Download Phone lists as a zip file",
                            data=fp,type='primary',
                            file_name=zip_filename,
                            mime="application/zip"
                        )
                if OP_SQL_CODE:
                  st.markdown('<h6 style="text-align: Left; box-sizing: border-box;color:black;">SQL Query:</h6>', unsafe_allow_html=True) 
                  st.code(("""
      """).join(rest_str.split('<br />')), language="sql", line_numbers=True,)
              st.session_state.submit_button_diasble=False
              logging_function(log_level='INFO',log_message=f'result presented')
            
          else:
            ref_str = "Please select atleast one of the output type!"
            st.info(ref_str)
      
    select_list = []
    join_list = []
    filter_list = []

    fin_query = """
    select 
    """