"""
Author: Victor Velasco
Name: main

Descritpion: This script is used for checking the value of one crypto currency like BTC
To any other coin like USD in real time
For change between the type of crypto currencies just change the value in the variable 
associated.
"""

#  Library imports
import pandas as pd
import sqlalchemy as sa  #  For interact with DB
import logging  # For create logs

#  Import funtions
from utils.utils import (
    get_coin_api_information,
    build_dataframe,
    connect_to_dwh,
    create_tbl_from_df,
    build_df_summary,
    calculate_summary_crypto,
    build_string_summary,
    send_email_alert,
)

# Config Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)


def extract_transform_load_crypto(
    table_name,
    base_currency,
    base_url,
    api_key,
    dwh_host,
    dwh_user,
    dwh_name,
    dwh_password,
    dwh_port,
    dwh_schema,
    executed_at,
    updated_at,
):
    """
    Proceso ETL para extracción de datos de cryptodivisas desde coinAPI para cargarlas en la tabla staging
    ->table_name: Nombre de la tabla donde se almacena las cryptodivisas
    ->base_currency: Moneda base para el precio de las cryptomonedas
    ->base_url: Url dado para la API de coinAPI
    ->api_key: API Key para consultar coinAPI
    ->dwh_host: Host del DataWarehouse
    ->dwh_user: Usuario del DataWarehouse
    ->dwh_name: Database name del DataWarehouse
    ->dwh_password: Password del DataWarehouse
    ->dwh_port: Puerto del DataWarehouse
    ->dwh_schema: Esquema donde se guardarán los datos dentro del DataWarehouse
    ->executed_at: Fecha de ejecución del proceso ETL
    ->updated_at: Fecha de actualización de los datos de la tabla historica crypto
    ->return: void
    """
    try:
        #  Get the JSON from API
        apiResponse = get_coin_api_information(
            base_currency=base_currency, base_url=base_url, api_key=api_key
        )

        #  Create a DataFrame from JSON and give format
        df = build_dataframe(apiResponse)

        #  Get engine connection to DataWareHouse
        engine = connect_to_dwh(
            dwh_host=dwh_host,
            dwh_name=dwh_name,
            dwh_user=dwh_user,
            dwh_port=dwh_port,
            dwh_password=dwh_password,
        )

        #  Insert DataFrame into Table stg
        create_tbl_from_df(
            df=df,
            table_name=table_name,
            engine=engine,
            schema=dwh_schema,
            executed_at=executed_at,
            updated_at=updated_at,
        )

    except Exception as e:
        logging.error(f"Error al obtener datos de {base_url}: {e}")
        raise e


def send_alert_summary(
    table_name,
    dwh_host,
    dwh_user,
    dwh_name,
    dwh_password,
    dwh_port,
    dwh_schema,
    updated_at,
    min_price,
    max_price,
    email_sender,
    email_receiver,
    email_smtp_secret,
    dag_name,
    ds,
):
    """
    Proceso de extracción de datos desde Redshift para calcular datos con cryptodivisas y obtener una alerta y enviarlo por correo al usuario
    ->table_name: Nombre de la tabla donde se almacena las cryptodivisas
    ->dwh_host: Host del DataWarehouse
    ->dwh_user: Usuario del DataWarehouse
    ->dwh_name: Database name del DataWarehouse
    ->dwh_password: Password del DataWarehouse
    ->dwh_port: Puerto del DataWarehouse
    ->dwh_schema: Esquema donde se guardarán los datos dentro del DataWarehouse
    ->updated_at: Fecha de actualización de los datos de la tabla historica crypto
    ->min_price: Precio mínimo deseado en el resumen de las cryptomonedas
    ->max_price: Precio máximo deseado en el resumen de las cryptomonedas
    ->email_sender: Remitente del correo electrónico de la alerta
    ->email_receiver: Destinatario del correo electrónico de alerta
    ->email_smtp_secret: Clave de acceso al servicio SMTP
    ->dag_name: Nombre del dag
    ->ds: Fecha de ejecución dada por el context del dag
    """
    try:
        #  Get engine connection to DataWareHouse
        engine = connect_to_dwh(
            dwh_host=dwh_host,
            dwh_name=dwh_name,
            dwh_user=dwh_user,
            dwh_port=dwh_port,
            dwh_password=dwh_password,
        )

        # Build and save DataFrames staging and history
        df_crypto_stg, df_crypto_hist = build_df_summary(
            table_name=table_name,
            schema=dwh_schema,
            updated_at=updated_at,
            engine=engine,
            min_price=min_price,
            max_price=max_price,
        )

        # Calculate summary data information
        (
            df_crypto_max_increment,
            df_crypto_min_increment,
            df_crypto_max_value,
        ) = calculate_summary_crypto(
            df_crypto_stg=df_crypto_stg, df_crypto_hist=df_crypto_hist
        )

        # Build String summary
        resume_message = build_string_summary(
            df_crypto_max_increment=df_crypto_max_increment,
            df_crypto_min_increment=df_crypto_min_increment,
            df_crypto_max_value=df_crypto_max_value,
        )

        # Send alert message using SMTP
        send_email_alert(
            resume_message=resume_message,
            email_sender=email_sender,
            email_receiver=email_receiver,
            email_smtp_secret=email_smtp_secret,
            dag_name=dag_name,
            ds=ds,
        )

    except Exception as e:
        logging.error(f"Error al construir mensaje:", {e})
        raise e
