from airflow import DAG
from airflow.models.connection import Connection

from datetime import datetime, timedelta
from codes.municipios_brasileiros import raw, refined, trusted

with DAG(
    "municipios_brasileiros",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    schedule="0 0 1 * *",
    start_date=datetime(2023, 8, 1),
    catchup=False,
) as dag:    
    s3a_connection = Connection.get_connection_from_secrets("s3a_connection")

    raw(s3a_connection.login, s3a_connection.password, s3a_connection.host) \
        >> refined(s3a_connection.login, s3a_connection.password, s3a_connection.host) \
            >> trusted(s3a_connection.login, s3a_connection.password, s3a_connection.host)
    