from airflow import DAG
from airflow.models.connection import Connection

from datetime import datetime, timedelta
from codes.clima_municipios_brasileiros import raw, raw_merged, refined, trusted

with DAG(
    "clima_municipios_brasileiros",
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    schedule="30 20 1 * *",
    start_date=datetime(2023, 8, 1),
    catchup=False,
) as dag:    
    s3a_connection = Connection.get_connection_from_secrets("s3a_connection")
    reference_date = (datetime.now() - timedelta(days=1))

    raw(s3a_connection.login, s3a_connection.password, s3a_connection.host, reference_date) \
        >> raw_merged(s3a_connection.login, s3a_connection.password, s3a_connection.host, reference_date) \
            >> refined(s3a_connection.login, s3a_connection.password, s3a_connection.host, reference_date) \
                >> trusted(s3a_connection.login, s3a_connection.password, s3a_connection.host, reference_date)
    