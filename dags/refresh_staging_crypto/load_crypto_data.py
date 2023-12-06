doc_md = """
## Refresh of staging schemas in Redshift


- DAG Name:
    *load_crypto_data*
- Owner:
    Victor Velasco
### Description:
    Este proceso realiza la extracción de datos de criptomonedas de La API CoinAPI,
    actualiza la información dada en la tabla stg y posteriormente la compara para cargar
    la información actualizada de las cryptodivisas en caso de que haya alguna actualización
    actualiza y muestra la fecha.
"""

import os
from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from utils.main import extract_transform_load_crypto
from utils.settings import LOCAL_TZ


# ---------- Globals ---------------
dag_id = "load_crypto_data"
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
dwh_schema=Variable.get("DB_SCHEMA")
api_key = Variable.get("API_KEY")
table_name = "crypto"
base_currency = "USD"
base_url = "https://rest.coinapi.io/v1/exchangerate"

# ------------- DAG -----------------------
with DAG(
    dag_id=dag_id,
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=schedule_interval,
    start_date=datetime(2023,12,1, tzinfo=LOCAL_TZ),
    description="Este proceso extrae información de las criptodivisas desde CoinAPI y las guarda en las tablas crypto & crypto_stg a las 00:00 HRS todos los días",
    catchup=True,
    doc_md=doc_md,
    tags=["staging"],
    template_searchpath=queries_base_path,
) as dag:
    # -------------- Tasks ----------------
    start_etl_process = BashOperator(
    task_id="start_etl_process",
    bash_command="echo 'Comenzando proceso ETL'"
    )
    
    create_tbl_crypto_stg = PostgresOperator(
        task_id="create_tbl_crypto_stg",
        postgres_conn_id="redshift_conn",
        sql="CREATE_STG_TBL_CRYPTO.sql",
        hook_params={	
            "options": f"-c search_path={dwh_schema}"
        }
    )
    
    create_tbl_crypto = PostgresOperator(
        task_id="create_tbl_crypto",
        postgres_conn_id="redshift_conn",
        sql="CREATE_TBL_CRYPTO.sql",
        hook_params={	
            "options": f"-c search_path={dwh_schema}"
        }
    )
    
    load_data_crypto = PythonOperator(
        task_id="load_stations_data",
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
            "executed_at":"'{{ data_interval_end | ds }}'",
            "updated_at":"'{{ data_interval_end | ts }}'",
        }
    )
    
    end_etl_process = BashOperator(
        task_id="end_etl_process",
        bash_command="echo 'Proceso ETL terminado'"
    )

# ---------------- Execution Order ------------------
start_etl_process >> create_tbl_crypto_stg
start_etl_process >> create_tbl_crypto

create_tbl_crypto_stg >> load_data_crypto
create_tbl_crypto >> load_data_crypto

load_data_crypto >> end_etl_process
