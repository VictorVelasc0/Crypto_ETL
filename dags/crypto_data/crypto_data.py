import os
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator
from utils.main import extract_transform_load_crypto, send_alert_summary
from utils.settings import LOCAL_TZ

doc_md = """
## Refresh of staging schemas in Redshift
## SUMARY:
-----
- DAG Name:
    `crypto_data`
- Owner:
    `Victor Velasco`
### Description:
    Este proceso realiza la extracción de datos de criptomonedas de La API CoinAPI;
    actualiza la información dada en la tabla stg y posteriormente la compara para cargar
    la información actualizada de las cryptodivisas en caso de que haya alguna actualización,
    actualiza y muestra la fecha de ejecución.
"""

# ---------- Globals ---------------
dag_id = "crypto_data"
schedule_interval = "@daily"
queries_base_path = os.path.join(os.path.dirname(__file__), "sql")
default_args = {
    "owner": "victor.velasco",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# -------- Variables ---------------------
dwh_host = Variable.get("DB_HOST")
dwh_user = Variable.get("DB_USER")
dwh_name = Variable.get("DB_NAME")
dwh_port = Variable.get("DB_PORT")
dwh_password = Variable.get("DB_PASSWORD")
dwh_schema = Variable.get("DB_SCHEMA")
api_key = Variable.get("API_KEY")
email_sender = Variable.get("EMAIL_SENDER")
email_receiver = Variable.get("EMAIL_RECEIVER")
email_smtp_secret = Variable.get("GMAIL_SMTP_SECRET")
table_name = "crypto"
base_currency = "USD"
base_url = "https://rest.coinapi.io/v1/exchangerate"
min_price = 0
max_price = 50000

# ------------- DAG -----------------------
with DAG(
    dag_id=dag_id,
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=schedule_interval,
    start_date=datetime(2023, 12, 1, tzinfo=LOCAL_TZ),
    description="Este proceso extrae información de las criptodivisas desde CoinAPI y las guarda en las tablas crypto & crypto_stg a las 00:00 HRS todos los días",
    catchup=True,
    doc_md=doc_md,
    tags=["staging"],
    template_searchpath=queries_base_path,
) as dag:
    # -------------- Tasks ----------------

    is_coin_api_available = HttpSensor(
        task_id="is_coin_api_available",
        http_conn_id="coin_api",
        endpoint="v1/exchangerate/BTC/USD",
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    start_etl_process = BashOperator(
        task_id="start_etl_process", bash_command="echo 'Comenzando proceso ETL'"
    )
    with TaskGroup("BUILD_TABLES_CRYPTO", prefix_group_id=False) as build_tables_crypto:
        create_tbl_crypto_stg = PostgresOperator(
            task_id="create_tbl_crypto_stg",
            postgres_conn_id="redshift_conn",
            sql="CREATE_STG_TBL_CRYPTO.sql",
            hook_params={"options": f"-c search_path={dwh_schema}"},
        )

        create_tbl_crypto = PostgresOperator(
            task_id="create_tbl_crypto",
            postgres_conn_id="redshift_conn",
            sql="CREATE_TBL_CRYPTO.sql",
            hook_params={"options": f"-c search_path={dwh_schema}"},
        )

    load_data_crypto = PythonOperator(
        task_id="load_crypto_data",
        python_callable=extract_transform_load_crypto,
        op_kwargs={
            "table_name": table_name,
            "base_currency": base_currency,
            "base_url": base_url,
            "dwh_host": dwh_host,
            "dwh_user": dwh_user,
            "dwh_name": dwh_name,
            "dwh_port": dwh_port,
            "dwh_schema": dwh_schema,
            "dwh_password": dwh_password,
            "api_key": api_key,
            "executed_at": "'{{ data_interval_end | ds }}'",
            "updated_at": "'{{ data_interval_end | ts }}'",
        },
    )

    send_email_alert = PythonOperator(
        task_id="send_email_alert",
        python_callable=send_alert_summary,
        op_kwargs={
            "table_name": table_name,
            "dwh_host": dwh_host,
            "dwh_user": dwh_user,
            "dwh_name": dwh_name,
            "dwh_port": dwh_port,
            "dwh_schema": dwh_schema,
            "dwh_password": dwh_password,
            "updated_at": "'{{ data_interval_end | ts }}'",
            "min_price": min_price,
            "max_price": max_price,
            "email_sender": email_sender,
            "email_receiver": email_receiver,
            "email_smtp_secret": email_smtp_secret,
            "dag_name": "{{ dag }}",
            "ds": "{{ ds }}",
        },
    )

    end_etl_process = BashOperator(
        task_id="end_etl_process", bash_command="echo 'Proceso ETL terminado'"
    )

# ---------------- Execution Order ------------------
is_coin_api_available >> start_etl_process

start_etl_process >> build_tables_crypto

build_tables_crypto >> load_data_crypto
build_tables_crypto >> load_data_crypto

load_data_crypto >> end_etl_process

end_etl_process >> send_email_alert
