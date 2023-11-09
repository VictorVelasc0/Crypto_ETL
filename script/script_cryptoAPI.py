'''
Author: Victor Velasco
Name: script_cryptoAPI

Descritpion: This script is used for checking the value of one crypto currency like BTC
To any other coin like USD in real time
For change between the type of crypto currencies just change the value in the variable 
associated.
'''

#  Library imports
import pandas as pd
import sqlalchemy as sa #  For interact with DB
import os
import sys


#  Import funtions
abs_path = os.path.dirname(os.path.abspath(__file__))
utils_path = os.path.join(os.path.dirname(__file__), "../utils")
sys.path.append(utils_path)
from utils import get_coin_api_information,build_dataframe,connect_to_dwh,sql_file,create_tbl_from_df


#  Get directory for ConfigParser and SQL files
config_path = os.path.join(os.path.dirname(__file__), "../config/config.ini")
sql_path = os.path.join(os.path.dirname(__file__), "../sql")


#  This is the script for create the DF and the table in redshift DWH

#  Get the JSON from API
apiResponse = get_coin_api_information(config_path = config_path,config_section="coin_api")

#  Create a DataFrame from JSON and give format
df = build_dataframe(apiResponse)

#  Get connection to DataWareHouse
conn = connect_to_dwh(config_path = config_path, config_section = "redshift")

#  Create Table stg
conn.execute(sql_file(sql_path = sql_path, sql = 'CREATE_STG_TBL_CRYPTO.sql'))

#  Create Table crypto
conn.execute(sql_file(sql_path = sql_path, sql = 'CREATE_TBL_CRYPTO.sql'))

#  Insert DataFrame into Table stg
create_tbl_from_df(df=df, name = "stg_crypto", con = conn, schema = "dani_gt_10_coderhouse", if_exists = "replace")

# SCD 1: Adding changes to Table Crypto
conn.execute(sql_file(sql_path = sql_path, sql = 'CREATE_TBL_CRYPTO.sql'))

# Close conection to DWH
conn.close()
