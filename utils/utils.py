"""
Author: Victor Velasco
Name: utils

Description: This file contains all the functions used by the application
"""

# Library imports
from sqlite3 import OperationalError
import requests  # For make an HTTP request
import pandas as pd
import logging  # For create logs
import sqlalchemy as sa  #  For interact with DB
import smtplib  # For send emails alerts


def get_coin_api_information(base_currency, base_url, api_key):
    """
    Esta funcion consulta el API de crypto usando:
    ->base_url:Url base de la api
    ->config_file: Archivo de configuración
    ->base_currency: Es la moneda en la cual queremos expresar la conversión del cripto ej: USD
    *Si la peticion no es 200 levanta un error de lo contrario devuelve un JSON con el response
    de la peticion.
    *Se hará 3 reintentos en caso de error cada 5 segundos
    ->return: JSON con respuesta
    """

    try:
        endpoint_url = f"{base_url}/{base_currency}"
        logging.info(f"Obteniendo datos desde URL API: {base_url}")
        logging.info(f"Moneda base: {base_currency}")
        params = {
            "asset_id_base": base_currency,
        }
        headers = {
            "X-CoinAPI-Key": api_key,
        }
        logging.warning(f"Consultando CoinAPI")
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  #  Levanta un exception si status no es 200
        logging.info(
            f"Datos obtenidos correctamente, status_code:{response.status_code}"
        )
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error en la petición {endpoint_url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error en la petición {endpoint_url}: {e}")
        return None


def build_dataframe(json_data):
    """
    Esta función construye un DataFrame a partir de un JSON
    ->json_data: Son los datos obtenidos desde la API
    ->return: Un DataFrame con los datos estructurados
    """
    try:
        logging.info(f"Construyendo Data Frame desde datos JSON")
        dfCripto = pd.json_normalize(json_data, "rates", ["asset_id_base"])
        logging.info(
            f"Reestructurando Data Frame para empatar la estructura de tablas en DWH"
        )
        dfCripto = dfCripto.rename(
            columns={
                "asset_id_base": "Base",
                "asset_id_quote": "Moneda",
                "rate": "Precio",
                "time": "created_at",
            }
        )
        dfCripto["Precio"] = 1 / dfCripto["Precio"]
        dfCripto["created_at"] = pd.to_datetime(
            dfCripto["created_at"], format="%Y-%m-%dT%H:%M:%S.%fZ"
        )
        logging.info(f"Data Frame creado:\n {dfCripto}")
        return dfCripto
    except Exception as e:
        logging.error(f"Error al contruir Data Frame: {e}")
        return None


def connect_to_dwh(dwh_host, dwh_name, dwh_user, dwh_port, dwh_password):
    """
    Esta función se usa para construir el string de conexion
    para conectarse posteriormente a DWH de redshift
    ->dwh_host: Hostname de DWH
    ->dwh_name: Nombre de la base de datos en el DWH
    ->dwh_user: Username de la base de datos en el DWH
    ->dwh_password: Contraseña del DWH
    return: conn: conexion a redshift engine:Motor de DB
    """

    #  Construye el string de conexión
    connetion_string = f"postgresql://{dwh_user}:{dwh_password}@{dwh_host}:{dwh_port}/{dwh_name}?sslmode=require"

    #  Se conecta a la DB
    try:
        logging.warning(f"Creando motor de conexión en {dwh_host} username: {dwh_user}")
        engine = sa.create_engine(connetion_string)
        logging.info(
            f"Motor de conexión creado exitosamente en {dwh_host} username: {dwh_user}"
        )
        return engine
    except sa.exc.OperationalError as e:
        logging.error(f"Error durante conexión {dwh_host}: {e}")
        raise OperationalError from e
    except sa.exc.RuntimeError as e:
        logging.error(
            f"Error se agotó el tiempo de intento de conexión {dwh_host}: {e}"
        )
        raise RuntimeError from e
    except Exception as e:
        logging.error(f"Error inesperado al conectar {dwh_host}: {e}")
        raise Exception from e


def create_tbl_from_df(df, table_name, schema, engine, executed_at, updated_at):
    """
    Esta función se usa para crear una tabla usando un DataFrame
    ->df: El DataFrame a convertir en tabla dentro de Redshift
    ->table_name: Nombre de la tabla
    ->schema: Nombre del esquema donde se va crear la tabla
    ->engine: motor de conexión a la DB de Redshift
    return: Void
    """

    try:
        logging.warning(f"Conectandose a la base de datos")
        with engine.connect() as conn, conn.begin():
            logging.info(f"Conectado exitosamente")
            logging.warning(
                f"Actualizando tabla {table_name}_stg a partir del información del Data Frame"
            )
            conn.execute(f"TRUNCATE TABLE {table_name}_stg")

            df.to_sql(
                f"{table_name}_stg",
                con=conn,
                schema=schema,
                if_exists="append",
                method="multi",
                index=False,
            )
            logging.info(f"Tabla: {table_name}_stg actualizada exitosamente")

            logging.warning(
                f"Actualizando tabla {table_name} a partir de {table_name}_stg"
            )
            logging.info(
                f"Aplicando SCD I (Slowly Changing Dimension) sobre {table_name}"
            )
            conn.execute(
                f"""
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
                """
            )
            logging.info(f"Tabla: {table_name} actualizada exitosamente")

    except Exception as e:
        logging.error(
            f"Error al intentar cargar el Data Frame en la tabla: {table_name} del esquema: {schema}",
            e,
        )
        raise Exception from e


def build_df_summary(table_name, schema, updated_at, engine, min_price, max_price):
    """
    Esta función construye dos DataFrame usando las tablas del DWH histórica sin considerar los registros más actuales
    ->table_name: Nombre de la tabla crypto o histórica (y staging al agregar _stg), esta tabla contiene tanto registros actuales como históricos
    ->schema: Nombre del esquema de donde se van a extraer los datos en Redshift
    ->updated_at: Fecha de la ultima actualización de la información cargada para cryptodivisas en Redshift
    ->min_price: Precio mínimo deseado en el resumen de las cryptomonedas
    ->max_price: Precio máximo deseado en el resumen de las cryptomonedas
    ->return: Dos DataFrame uno para staging y otro de crypto histórico
    """
    try:
        logging.warning(f"Conectandose a la base de datos")
        with engine.connect() as conn, conn.begin():
            logging.info(f"Conectado exitosamente")

            crypto_stg = f"SELECT moneda, base, AVG(precio) AS precio FROM {schema}.{table_name}_stg GROUP BY moneda,base"
            crypto_hist = f"SELECT moneda, base, AVG(precio) AS precio FROM {schema}.{table_name} WHERE updated_at::date != {updated_at} GROUP BY moneda,base"

            logging.warning(f"Extrayendo datos de DWH para {table_name}_stg")
            df_crypto_stg = pd.read_sql_query(crypto_stg, conn)
            logging.warning(
                f"Aplicando filtros de precios para {table_name}_stg, precio máximo del resumen: {max_price}, precio mínimo del resumen: {min_price}"
            )
            df_crypto_stg = df_crypto_stg[
                (df_crypto_stg["precio"] >= min_price)
                & (df_crypto_stg["precio"] <= max_price)
            ]
            logging.info(f"Datos extraídos con éxito para {table_name}_stg")
            print(df_crypto_stg)
            logging.warning(f"Extrayendo datos de DWH para {table_name}")
            df_crypto_hist = pd.read_sql_query(crypto_hist, conn)
            logging.warning(
                f"Aplicando filtros de precios para {table_name}, precio máximo del resumen: {max_price}, precio mínimo del resumen: {min_price}"
            )
            df_crypto_hist = df_crypto_hist[
                (df_crypto_hist["precio"] >= min_price)
                & (df_crypto_hist["precio"] <= max_price)
            ]
            logging.info(f"Datos extraídos con éxito para {table_name}")

            return df_crypto_stg, df_crypto_hist

    except Exception as e:
        logging.error(
            f"Error al intentar extraer los datos de las tablas {table_name} y {table_name}_stg del DWH",
            e,
        )
        raise Exception from e


def calculate_summary_crypto(df_crypto_stg, df_crypto_hist):
    """
    Esta función se utiliza para calcular el resumen de los datos de las cryptomonedas al día
    ->df_crypto_stg: DataFrame construido a partir de la tabla de datos de crypto staging
    ->df_crypto_hist: DataFrame construido a partir de la tabla con información histórica de la tabla crypto
    return: Dataframe:
        ->df_crypto_max_increment: Máximo porcentaje de incremento en precio respecto al precio promedio histórico
        ->df_crypto_min_increment: Mñinimo porcentaje de incremento en precio respecto al precio promedio histórico
        ->df_crypto_max_value: 5 Cryptomonedas con el precio más alto
    """
    try:
        logging.warning(f"Uniendo los datos del Dataframe Staging con el histórico")
        merged_df = pd.merge(
            df_crypto_stg, df_crypto_hist, on="moneda", suffixes=("_stg", "_hist")
        )
        logging.info(f"DataFrames unidos exitosamente")

        logging.warning(
            f"Calculando el porcentaje de cambio entre los datos actuales y los históricos de precio en crypto"
        )
        merged_df["porcentaje_cambio"] = (
            (merged_df["precio_stg"] - merged_df["precio_hist"])
            / merged_df["precio_hist"]
        ) * 100
        logging.info(f"Porcentaje de cambio calculado exitósamente")

        logging.warning(
            f"Guardando DataFrame de las 5 cryptomonedas con mayor porcentaje de incremento en precio"
        )
        df_crypto_max_increment = merged_df.sort_values(
            by=["porcentaje_cambio"], ascending=False
        ).head(5)
        logging.warning(
            f"Guardando DataFrame de las 5 cryptomonedas con menor porcentaje de incremento en precio"
        )

        df_crypto_min_increment = merged_df.sort_values(
            by=["porcentaje_cambio"], ascending=True
        ).head(5)
        logging.info(f"Porcentajes calculados exitósamente")
        print(
            df_crypto_max_increment[
                ["moneda", "precio_stg", "precio_hist", "base_stg", "porcentaje_cambio"]
            ]
        )
        print(
            df_crypto_min_increment[
                ["moneda", "precio_stg", "precio_hist", "base_stg", "porcentaje_cambio"]
            ]
        )

        logging.warning(f"Guardando DataFrame de las 5 cryptomonedas con mayor precio")
        df_crypto_max_value = df_crypto_stg.sort_values(
            by=["precio"], ascending=False
        ).head(5)
        logging.info(f"Valores guardados exitósamente")
        print(df_crypto_max_value[["moneda", "precio", "base"]])

        return df_crypto_max_increment, df_crypto_min_increment, df_crypto_max_value

    except Exception as e:
        logging.error(
            f"Error al intentar construir el resumen a partir de los DataFrame", e
        )
        raise Exception from e


def build_string_summary(
    df_crypto_max_increment, df_crypto_min_increment, df_crypto_max_value
):
    """
    Esta función construye el mensaje principal que es enviado por correo a los usuarios por SMTP
    ->df_crypto_max_increment: Máximo porcentaje de incremento en precio respecto al precio promedio histórico
    ->df_crypto_min_increment: Mñinimo porcentaje de incremento en precio respecto al precio promedio histórico
    ->df_crypto_max_value: 5 Cryptomonedas con el precio más alto
    ->return: String con un resumen de los datos de las 5 cryptomonedas que tuvieron un porcentaje de aumento
            en precio mayor respecto al promedio histórico y aquellos que tienen menor aumento así como las monedas
            con mayor valor y menor valor al día
    """
    try:
        logging.warning(f"Generando mensaje de resumen diario")
        # Mensaje de saludo y explicación
        introduction_message = (
            "Hola! Aqui tienes el resumen del dia de las cryptomonedas.\n\n"
        )

        # Crear strings informativos
        info_max_increment = "Las Cryptomonedas que tuvieron mayor incremento son:\n\n"
        for idx, row in df_crypto_max_increment.iterrows():
            info_max_increment += f"{idx + 1}.  {row['moneda']} tuvo un aumento del {row['porcentaje_cambio']:.2f}% con un precio {row['precio_hist']:.8f} {row['base_hist']} -> {row['precio_stg']:.8f} {row['base_stg']}\n"

        info_min_increment = (
            "\nLas Cryptomonedas que tuvieron menor incremento son:\n\n"
        )
        for idx, row in df_crypto_min_increment.iterrows():
            info_min_increment += f"{idx + 1}.  {row['moneda']} tuvo una disminucion del {row['porcentaje_cambio']:.2f}% con un precio {row['precio_hist']:.8f} {row['base_hist']} -> {row['precio_stg']:.8f} {row['base_stg']}\n"

        info_max_value = "\nLas Cryptomonedas con los precios mas altos son:\n\n"
        for idx, row in df_crypto_max_value.iterrows():
            info_max_value += f"{idx + 1}.  {row['moneda']} con un precio {row['precio']:.2f} {row['base']}\n"

        # Mensaje persuasivo para el cliente
        goodbye_message = "\nEste es un resumen del dia de las cryptomonedas.\n\nTe invitamos a revisar tu wallet y considerar estas oportunidades de inversion para maximizar tus ganancias.\nNo pierdas la oportunidad de invertir en estas cryptomonedas en alza!"

        # Unir todo en un solo string
        resume_message = (
            introduction_message
            + info_max_increment
            + info_min_increment
            + info_max_value
            + goodbye_message
        )
        logging.info(f"Mensaje de resumen diario construido exitósamente")

        return resume_message

    except Exception as e:
        logging.error(
            f"Error al intentar construir el resumen a partir de los DataFrame", e
        )
        raise Exception from e


def send_email_alert(
    resume_message, email_sender, email_receiver, email_smtp_secret, dag_name, ds
):
    """
    Esta función se encarga de enviar el mensaje usando SMTP al destinatario deseado usando un correo base
    ->resume_message: Mensaje con los datos del resumen de cryptodivisas a enviar
    ->email_sender: Email que envía el mensaje
    ->email_receiver: Email que recibe el mensaje
    ->email_smtp_secret: Contraseña del servicio SMTP
    ->_dag_name: Nombre del dag
    ->ds: Fecha de ejecución del dag_name
    return: void
    """
    try:
        logging.warning(f"Conectandose al servicio SMTP")
        obj_smtp = smtplib.SMTP("smtp.gmail.com", 587)
        obj_smtp.starttls()
        obj_smtp.login(email_sender, email_smtp_secret)
        logging.info(f"Conectando exitósamente al servicio SMTP usando {email_sender}")

        logging.warning(f"Construyendo mensaje de alerta")
        subject = "Subject: {} - {}\n\n".format(dag_name, ds)
        message = subject + "\n\n" + resume_message
        logging.info(f"Mensaje contruido")
        print(message)
        logging.warning(
            f"Enviando mensaje {subject}, remitente: {email_sender}, destinatario: {email_receiver}"
        )
        obj_smtp.sendmail(email_sender, email_receiver, message)
        logging.info(f"Mensaje enviado exitósamente a {email_sender}")

    except Exception as e:
        logging.error(f"Error al intentar enviar la alerta de correo electrónico", e)
        raise Exception from e
