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
from retrying import retry # For making retries some part of code like API get
from configparser import ConfigParser #  For reading the API KEY in config.ini folder

# Config Logging
logging.basicConfig(filename='./logs/crypto_etl.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%H:%M:%S')

@retry(wait_fixed=5000,stop_max_attempt_number=3)
def get_coin_api_information(config_path,config_section):
    '''
    Esta funcion consulta el API de crypto usando
    ->base_url:Url base de la api
    ->config_dir: Ubicacion del directorio de configuracion
    ->base_currency: Es la moneda en la cual queremos expresar la conversión del cripto ej: USD
    *Si la peticion no es 200 levanta un error de lo contrario devuelve un JSON con el response 
    de la peticion.
    *Se hará 3 reintentos en caso de error cada 5 segundos
    '''

    try:
        config = ConfigParser()
        config.read(config_path)
        if config.has_section(config_section):
            logging.info(f"Obteniendo credenciales de Redshift")
            api_key = config[config_section]["api_key"]
            base_url = config[config_section]["base_url"]
            logging.info(f"base URL API: {base_url}")
            base_currency = config[config_section]["base_currency"]
            logging.info(f"base currency: {base_currency}")
            url = f"{base_url}/{base_currency}"
            params = {
                "asset_id_base":base_currency,
            }
            headers = {
                "X-CoinAPI-Key": api_key,
            }
            logging.warning(f"Obteniendo datos desde coinAPI")
            response = requests.get(url,params=params, headers=headers)
            response.raise_for_status() #  Levanta un exception si status no es 200
            logging.info(f"Datos obtenidos respuesta, status_code:{response.status_code}")
            return response.json()
        else:
            raise Exception(f"No se encontró la sección: {config_section} en el archivo: {config_path}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al consultar la API: {e}")
        return None
    except Exception as e:
        logging.error(f"Error al consultar la API: {e}")
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
        logging.info(f"Reestructurando datos del Data Frame")
        dfCripto = dfCripto.rename(columns = {'asset_id_base':'Base', 'asset_id_quote':'Moneda','rate':'Precio', 'time':'created_at'})
        dfCripto['created_at'] = pd.to_datetime(dfCripto['created_at'], format = '%Y-%m-%dT%H:%M:%S.%fZ')
        logging.info(f"Data Frame creado:\n {dfCripto}")
        return dfCripto
    except Exception as e:
        logging.error(f"Error al contruir Data Frame: {e}")
        return None

@retry(wait_fixed=5000,stop_max_attempt_number=3)
def connect_to_dwh(config_path, config_section):
    '''
    Esta función se usa para construir el string de conexion 
    para conectarse posteriormente a DWH de redshift
    ->config_path: Ubicacion del directorio de configuracion
    ->config_section: seccion a buscar variables en archivo configuracion
    return: conn: conexion a redshift engine:Motor de DB
    *Se hará 3 reintentos en caso de error cada 5 segundos
    '''
    config = ConfigParser()
    config.read(config_path)
    host = config[config_section]["host"]
    port = config[config_section]["port"]
    username = config[config_section]["username"]
    dbname = config[config_section]["dbname"]
    password = config[config_section]["password"]

    #  Construye el string de conexión
    connetion_string = f'postgresql://{username}:{password}@{host}:{port}/{dbname}?sslmode=require'

    #  Se conecta a la DB
    try:
        logging.warning(f"Conectandose al Data Warehouse username: {username}")
        engine = sa.create_engine(connetion_string)
        conn = engine.connect()
        logging.info(f"Conectado exitosamente al Data Warehouse username: {username}")
        return conn
    except sa.exc.OperationalError as e:
        logging.error(f"Error durante conexión al Data Warehouse: {e}")
        raise OperationalError from e
    except sa.exc.RuntimeError as e:
        logging.error(f"Error se agotó el tiempo de intento de conexión al Data Warehouse: {e}")
        raise RuntimeError from e
    except Exception as e:
        logging.error(f"Error inesperado al conectar al Data Warehouse: {e}")
        raise Exception from e
    
def sql_file(sql_path,sql):
    '''
    Esta funcion se usa para devolver un string de texto de un codigo SQL
    ->sql_path: Directorio donde se almacenan los archivos.sql
    ->sql_file: Nombre del archivo.sql
    ->return archivo en formato string del SQL a ejecutar
    '''
    try:
        logging.info(f"Construyendo string a partir del archivo SQL: {sql} en: {sql_path}")
        with open(f'{sql_path}/{sql}', 'r') as archivo_sql:
            logging.info(f"Construcción de SQL a string completa")
            logging.warning(f"Construyendo tabla en Data Warehouse")
            logging.info(f"Construcción completa")
            return archivo_sql.read()
    except Exception as e:
        logging.error(f"Error al leer archivo: {sql} desde: {sql_path}: {e}")

def create_tbl_from_df(df,name,schema,con,if_exists):
    '''
    Esta función se usa para crear una tabla usando un DataFrame
    ->df: El DataFrame a convertir en tabla dentro de Redshift
    ->name: Nombre de la tabla
    ->schema: Nombre del esquema donde se va crear la tabla
    ->con: Objecto de conexión a la DB de Redshift
    ->if_exist: Indica que se va hacer en caso de que la tabla exista
        replace; Reemplaza la tabla 
        append: Agrega los datos a la tabla existente
    '''
    
    try:
        logging.warning(f"Cargando el Data Frame en la tabla: {name} del esquema: {schema} usando: {if_exists} en Data Warehouse")
        df.to_sql(
            name = name,
            con = con,
            schema = schema,
            if_exists = if_exists,
            method = "multi",
            index = False,
        )
        logging.info(f"El Data Frame ha sido cargado exitosamente en la tabla: {name} del esquema: {schema}")
    except Exception as e:
        logging.error(f"Error al intentar cargar el Data Frame en la tabla: {name} del esquema: {schema}")
        raise Exception from e