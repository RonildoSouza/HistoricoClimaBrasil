import logging
from airflow.decorators import task

@task()
def raw(s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str):
    import requests
    from codes.utils_datalake import upload_file
    
    logging.info("DATA EXTRACT")
    estados_response = requests.get("https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/estados.csv")
    municipios_response = requests.get("https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv")

    for response in [("estados", estados_response), ("municipios", municipios_response)]:
        response[1].raise_for_status()
        
        upload_file(
            bucket_prefix="raw/municipios_brasileiros",
            filename_with_extension=f"{response[0]}.csv",
            filebytes=response[1].text.encode('UTF-8'),
            s3a_access_key=s3a_access_key,
            s3a_secret_key=s3a_secret_key,
            s3a_endpoint=s3a_endpoint
        )
    return "SUCESSO"

@task()
def refined(s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str):
    from codes.utils_datalake import spark_session_builder

    spark = spark_session_builder(
        s3a_access_key=s3a_access_key, 
        s3a_secret_key=s3a_secret_key, 
        s3a_endpoint=s3a_endpoint, 
        app_name="refined_municipios_brasileiros"
    )

    logging.info("DATA EXTRACT READ")
    estados = spark.read.csv(
        path="s3a://datalake/raw/municipios_brasileiros/estados.csv.gz", 
        schema="codigo_uf LONG, uf STRING, nome STRING, latitude FLOAT, longitude FLOAT, regiao STRING",
        header=True
    )

    municipios = spark.read.csv(
        path="s3a://datalake/raw/municipios_brasileiros/municipios.csv.gz", 
        schema="codigo_ibge LONG, nome STRING, latitude FLOAT, longitude FLOAT, capital INT, codigo_uf LONG",
        header=True
    )

    logging.info("DATA TRANSFORM")
    refined = (
        municipios.alias("m").join(
            other=estados.alias("e"),
            on=(municipios.codigo_uf == estados.codigo_uf),
            how="inner"
        )
        .selectExpr([
            "m.codigo_ibge",
            "m.nome",
            "m.latitude",
            "m.longitude",
            "(m.capital == 1) AS e_capital",
            "e.uf",
            "e.nome AS nome_estado",
            "e.regiao",
            "current_timestamp() AS gerado_em",
        ])
        .distinct()
    )

    logging.info("DATA WRITE")
    (
        refined.write
        .option("overwriteSchema", "true")
        .format("delta")
        .mode("overwrite")
        .save("s3a://datalake/refined/municipios_brasileiros")
    )

    if spark:
        spark.stop()

    return "SUCESSO"

@task()
def trusted(s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str):
    from codes.utils_datalake import spark_session_builder

    spark = spark_session_builder(
        s3a_access_key=s3a_access_key, 
        s3a_secret_key=s3a_secret_key, 
        s3a_endpoint=s3a_endpoint, 
        app_name="trusted_municipios_brasileiros"
    )

    logging.info("DATA READ REFINED")
    refined = (
        spark.read.load(
            path="s3a://datalake/refined/municipios_brasileiros",
            format="delta",
        )
        .selectExpr([
            "codigo_ibge",
            "nome",
            "e_capital",
            "uf",
            "nome_estado",
            "regiao",
            "current_timestamp() AS gerado_em"
        ])
    )

    logging.info("DATA WRITE")
    (
        refined.write
        .option("overwriteSchema", "true")
        .format("delta")
        .mode("overwrite")
        .save("s3a://datalake/trusted/municipios_brasileiros")
    )

    if spark:
        spark.stop()