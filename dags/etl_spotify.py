# Este es el DAG que orquesta el ETL de la tabla popular_songs
from os import environ as env
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.models import Variable

from datetime import datetime, timedelta


REDSHIFT_SCHEMA = env["REDSHIFT_SCHEMA"]

QUERY_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.popular_songs(
    id_song VARCHAR(250) NOT NULL,
    song_name VARCHAR(250) NOT NULL,
    artist VARCHAR(250) NOT NULL,
    album VARCHAR(150) NOT NULL,
    popularity INTEGER NOT NULL,
    duration_ms INTEGER NULL,
    song_link VARCHAR(250) NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    timestamp_ TIMESTAMP NOT NULL,
    alternate_key VARCHAR(255) NOT NULL,
    PRIMARY KEY (alternate_key))
    distkey(artist)
    compound sortkey(timestamp_, country_code);
"""


defaul_args = {
    "owner": "Lautar Poletto",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_spotify",
    default_args=defaul_args,
    description="ETL de la tabla popular_songs",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Tareas
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )


    spark_etl_spotify = SparkSubmitOperator(
        task_id="spark_etl_spotify",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_Spotify.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    create_table >> spark_etl_spotify