'''
Author: Victor Velasco
Name: script_cryptoAPI

Description: This file contains all the functions used by the application
'''
# Library imports
from sqlite3 import OperationalError
import requests
import pandas as pd
import logging # For create logs
import sqlalchemy as sa #  For interact with DB

def get_coin_api_information( base_currency, base_url, api_key):
    '''
    Esta funcion consulta el API de crypto usando:
    ->base_url:Url base de la api
    ->config_file: Archivo de configuración
    ->base_currency: Es la moneda en la cual queremos expresar la conversión del cripto ej: USD
    *Si la peticion no es 200 levanta un error de lo contrario devuelve un JSON con el response 
    de la peticion.
    *Se hará 3 reintentos en caso de error cada 5 segundos
    ->return: JSON con respuesta
    '''

    try:
        endpoint_url = f"{base_url}/{base_currency}"
        logging.info(f"Obteniendo datos desde URL API: {base_url}")
        logging.info(f"Moneda base: {base_currency}")
        params = {
            "asset_id_base":base_currency,
        }
        headers = {
            "X-CoinAPI-Key": api_key,
        }
        logging.warning(f"Consultando CoinAPI")
        response = requests.get(endpoint_url,params=params, headers=headers)
        response.raise_for_status() #  Levanta un exception si status no es 200
        logging.info(f"Datos obtenidos correctamente, status_code:{response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error en la petición {endpoint_url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error en la petición {endpoint_url}: {e}")
        return None

def build_dataframe(json_data):
    '''
    Esta función construye un DataFrame a partir de un JSON
    ->json_data: Son los datos obtenidos desde la API
    ->return: Un DataFrame con los datos estructurados
    '''
    try:
        logging.info(f"Construyendo Data Frame desde datos JSON")
        dfCripto = pd.json_normalize(json_data, 'rates', ['asset_id_base'])
        logging.info(f"Reestructurando Data Frame para empatar la estructura de tablas en DWH")
        dfCripto = dfCripto.rename(columns = {'asset_id_base':'Base', 'asset_id_quote':'Moneda','rate':'Precio', 'time':'created_at'})
        dfCripto['Precio'] = 1/dfCripto['Precio']
        dfCripto['created_at'] = pd.to_datetime(dfCripto['created_at'], format = '%Y-%m-%dT%H:%M:%S.%fZ')
        logging.info(f"Data Frame creado:\n {dfCripto}")
        return dfCripto
    except Exception as e:
        logging.error(f"Error al contruir Data Frame: {e}")
        return None

def connect_to_dwh(dwh_host, dwh_name, dwh_user, dwh_port, dwh_password):
    '''
    Esta función se usa para construir el string de conexion 
    para conectarse posteriormente a DWH de redshift
    ->dwh_host: Hostname de DWH
    ->dwh_name: Nombre de la base de datos en el DWH
    ->dwh_user: Username de la base de datos en el DWH
    ->dwh_password: Contraseña del DWH
    return: conn: conexion a redshift engine:Motor de DB
    '''

    #  Construye el string de conexión
    connetion_string = f'postgresql://{dwh_user}:{dwh_password}@{dwh_host}:{dwh_port}/{dwh_name}?sslmode=require'

    #  Se conecta a la DB
    try:
        logging.warning(f"Conectandose {dwh_host} username: {dwh_user}")
        engine = sa.create_engine(connetion_string)
        conn = engine.connect()
        logging.info(f"Conectado exitosamente {dwh_host} username: {dwh_user}")
        return conn
    except sa.exc.OperationalError as e:
        logging.error(f"Error durante conexión {dwh_host}: {e}")
        raise OperationalError from e
    except sa.exc.RuntimeError as e:
        logging.error(f"Error se agotó el tiempo de intento de conexión {dwh_host}: {e}")
        raise RuntimeError from e
    except Exception as e:
        logging.error(f"Error inesperado al conectar {dwh_host}: {e}")
        raise Exception from e

def create_tbl_from_df(df,table_name,schema,con, executed_at, updated_at):
    '''
    Esta función se usa para crear una tabla usando un DataFrame
    ->df: El DataFrame a convertir en tabla dentro de Redshift
    ->table_name: Nombre de la tabla
    ->schema: Nombre del esquema donde se va crear la tabla
    ->con: Objecto de conexión a la DB de Redshift
    return: Void
    '''
    
    try:
        logging.warning(f"Atualizando tabla {table_name}_stg a partir del información del Data Frame")
        con.execute(f"TRUNCATE TABLE {table_name}_stg")
        df.to_sql(
            f"{table_name}_stg",
            con = con,
            schema = schema,
            if_exists = "append",
            method = "multi",
            index = False,
        )
        logging.info(f"Tabla: {table_name}_stg actualizada exitosamente")
        logging.warning(f"Actualizando tabla {table_name} a partir de {table_name}_stg")
        logging.info(f"Aplicando SCD I (Slowly Changing Dimension) sobre {table_name}")
        con.execute(f"""
            BEGIN;
            MERGE INTO {schema}.{table_name}
            USING {schema}.{table_name}_stg
            ON {table_name}.Moneda={table_name}_stg.Moneda AND {table_name}.created_at={table_name}_stg.created_at
            WHEN MATCHED THEN
                UPDATE SET 
                Precio = {table_name}_stg.Precio,
                created_at = {table_name}_stg.created_at,
                updated_at = {updated_at},
                executed_at = {executed_at}
            WHEN NOT MATCHED THEN
                INSERT (Moneda, Base, Precio, created_at, updated_at, executed_at)
                VALUES ({table_name}_stg.Moneda, {table_name}_stg.Base, {table_name}_stg.Precio, {table_name}_stg.created_at, {updated_at}, {executed_at});
            COMMIT;
            """)
        logging.info(f"Tabla: {table_name} actualizada exitosamente")
    except Exception as e:
        logging.error(f"Error al intentar cargar el Data Frame en la tabla: {table_name} del esquema: {schema}", e)
        raise Exception from e
