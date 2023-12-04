'''
Author: Victor Velasco
Name: script_cryptoAPI

Descritpion: This script is used for checking the value of one crypto currency like BTC
To any other coin like USD in real time
For change between the type of crypto currencies just change the value in the variable 
associated.
'''

#  Library imports
import os
import sys
import pandas as pd
import sqlalchemy as sa #  For interact with DB
import logging # For create logs
#  Import funtions
from utils.utils import get_coin_api_information, build_dataframe, connect_to_dwh, create_tbl_from_df

# Config Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%H:%M:%S')

def extract_transform_load_crypto(table_name, base_currency, base_url, dwh_host, dwh_user, dwh_name, dwh_password, dwh_port, dwh_schema, api_key, executed_at, updated_at):
    """
    Proceso ETL para extracción de datos de cryptodivisas desde coinAPI para cargarlas en la tabla staging
    ->config_path: Archivo de configuraión para consultar el url y API key
    ->return: void
    """
    try:
        #  Get the JSON from API
        apiResponse = get_coin_api_information(base_currency=base_currency, base_url=base_url, api_key=api_key)

        #  Create a DataFrame from JSON and give format
        df = build_dataframe(apiResponse)

        #  Get connection to DataWareHouse
        conn = connect_to_dwh(dwh_host=dwh_host, dwh_name=dwh_name, dwh_user=dwh_user, dwh_port=dwh_port, dwh_password=dwh_password)

        #  Insert DataFrame into Table stg
        create_tbl_from_df(df=df, table_name=table_name, con=conn, schema=dwh_schema, executed_at=executed_at, updated_at=updated_at)

        # Close conection to DWH
        conn.close()
    except Exception as e:
        logging.error(f"Error al obtener datos de {base_url}: {e}")
        raise e
