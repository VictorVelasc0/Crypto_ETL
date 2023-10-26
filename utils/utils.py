'''
Author: Victor Velasco
Name: script_cryptoAPI

Description: This file contains all the functions used by the application
'''
#Library imports
import requests
import pandas as pd
import sqlalchemy as sa #  For interact with DB
from configparser import ConfigParser #  For reading the API KEY in config.ini folder

def get_coin_to_usd(base_url, config_path, base_currency):
    '''
    Esta funcion consulta el API de crypto usando
    ->base_url:Url base de la api
    ->config_dir: Ubicacion del directorio de configuracion
    ->base_currency: Es la moneda en la cual queremos expresar la conversión del cripto ej: USD
    *Si la peticion no es 200 levanta un error de lo contrario devuelve un JSON con el response 
    de la peticion.
    '''

    try:
        config=ConfigParser()
        config.read(config_path)
        api_key=config["coin_api"]["api_key"]
        url = f"{base_url}/{base_currency}"
        params={
            "asset_id_base":base_currency,
        }
        headers = {
            "X-CoinAPI-Key": api_key,
        }

        response = requests.get(url,params=params, headers=headers)
        response.raise_for_status() #  Levanta un exception si status no es 200
        return response.json()
    
    
    except requests.exceptions.RequestException as e:
        #  Captura cualquier error durante la solicitud
        print(f" La peticion ha fallado. Codigo de error: {e}")
        return None

def build_table(json_data):
    '''
    Esta función construye un DataFrame a partir de un JSON
    ->json_data: Son los datos obtenidos desde la API
    ->return: Un DataFrame con los datos estructurados
    '''

    dfCripto = pd.json_normalize(json_data, 'rates', ['asset_id_base'])
    dfCripto = dfCripto.rename(columns = {'asset_id_base':'Base','asset_id_quote':'Moneda','rate':'Precio','time':'checked_at'})
    dfCripto['checked_at'] = pd.to_datetime(dfCripto['checked_at'], format='%Y-%m-%dT%H:%M:%S.%fZ')
    dfCripto['checked_at'] = dfCripto['checked_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return dfCripto


def connect_to_dwh(config_path, config_section):
    '''
    Esta función se usa para construir el string de conexion 
    para conectarse posteriormente a DWH de redshift
    ->config_dir: Ubicacion del directorio de configuracion
    return: conn: conexion a redshift engine:Motor de DB
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
        engine = sa.create_engine(connetion_string)
        conn = engine.connect()
        return conn, engine
    except sa.exc.OperationalError as e:
        print(f"Error durante conexión con la base de datos: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")
        # Maneja otros errores inesperados aquí, si es necesario.
    
def sql_file(sql_path,sql):
    '''
    Esta funcion se usa para devolver un string de texto de un codigo SQL
    ->sql_path: Directorio donde se almacenan los archivos.sql
    ->sql_file:Nombre del archivo.sql
    ->return archivo en formato string del SQL a ejecutar
    '''

    with open(f'{sql_path}/{sql}', 'r') as archivo_sql:
        return archivo_sql.read()
