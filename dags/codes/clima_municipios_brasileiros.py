import logging
from datetime import datetime
from airflow.decorators import task

@task()
def raw(s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str, reference_date: datetime):
    import json
    import requests
    import calendar
    from uuid import uuid4
    from codes.utils_datalake import spark_session_builder, upload_file
    
    logging.info("READ EXTRACT municipios_brasileiros")    
    spark = spark_session_builder(
        s3a_access_key=s3a_access_key, 
        s3a_secret_key=s3a_secret_key, 
        s3a_endpoint=s3a_endpoint, 
        app_name="raw_clima_municipios_brasileiros"
    )

    municipios = (
        spark.read
        .format("delta")
        .load("s3a://datalake/refined/municipios_brasileiros")
    )

    logging.info("DATAFRAME TO LIST OF ROW") 
    municipios_collect = municipios.orderBy("regiao", "codigo_ibge").collect()

    if spark:
        spark.stop()
    
    logging.info("DATA EXTRACT")
    last_day_in_month = calendar.monthrange(reference_date.year, reference_date.month)[1]
    start_date = f"{reference_date.strftime('%Y-%m')}-01"
    end_date = f"{reference_date.strftime('%Y-%m')}-{last_day_in_month}"

    for row in municipios_collect:
        response = requests.get(f"https://archive-api.open-meteo.com/v1/era5?latitude={row.latitude}&longitude={row.longitude}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&timezone=America/Sao_Paulo")
        response.raise_for_status()
        
        upload_file(
            bucket_prefix=f"raw/clima_municipios_brasileiros/{reference_date.strftime('%Y/%m')}",
            filename_with_extension=f"{row.codigo_ibge}_{datetime.today().strftime('%Y%m%d%H%M%S')}_{uuid4()}.json",
            filebytes=json.dumps({ "codigo_ibge": row.codigo_ibge, "payload": response.json() }).encode('utf-8'),
            s3a_access_key=s3a_access_key,
            s3a_secret_key=s3a_secret_key,
            s3a_endpoint=s3a_endpoint
        )

    return "SUCESSO"

@task()
def raw_merged(s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str, reference_date: datetime):
    from codes.utils_datalake import spark_session_builder
    
    spark = spark_session_builder(
        s3a_access_key=s3a_access_key, 
        s3a_secret_key=s3a_secret_key, 
        s3a_endpoint=s3a_endpoint, 
        app_name="raw_merged_clima_municipios_brasileiros"
    )

    base_path = f"s3a://datalake/raw/clima_municipios_brasileiros/{reference_date.strftime('%Y/%m')}"
    raw = spark.read.json(base_path)
    raw.printSchema()

    logging.info(f"DATA WRITE")
    (
        raw.write
        .format("parquet")
        .mode("overwrite")
        .save(f"{base_path}/merged")
    )

    if spark:
        spark.stop()

    return "SUCESSO"

@task()
def refined(s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str, reference_date: datetime):
    from codes.utils_datalake import spark_session_builder

    spark = spark_session_builder(
        s3a_access_key=s3a_access_key, 
        s3a_secret_key=s3a_secret_key, 
        s3a_endpoint=s3a_endpoint, 
        app_name="refined_clima_municipios_brasileiros"
    )

    clima_municipios = spark.read.parquet(f"s3a://datalake/raw/clima_municipios_brasileiros/{reference_date.strftime('%Y')}/*/merged")

    clima_municipios.createOrReplaceTempView("clima_municipios")
    clima_municipios.printSchema()

    datasets_and_queries = [
        {
            "dataset": "temporalidade",
            "query": """
                WITH _time AS (
                    SELECT DISTINCT
                        codigo_ibge,
                        posexplode(payload.hourly.time) AS (idx, time),
                        payload.elevation,
                        payload.latitude,
                        payload.longitude,
                        payload.timezone,
                        payload.timezone_abbreviation,
                        payload.utc_offset_seconds
                    FROM clima_municipios
                )
                SELECT DISTINCT
                    _time.idx,
                    _time.codigo_ibge,
                    CAST(_time.time AS TIMESTAMP) AS time,
                    _time.elevation,
                    _time.latitude,
                    _time.longitude,
                    _time.timezone,
                    _time.timezone_abbreviation,
                    _time.utc_offset_seconds
                FROM _time
            """
        },
        {
            "dataset": "humidade",
            "query": """
                WITH _relativehumidity AS (
                    SELECT DISTINCT
                        codigo_ibge,
                        posexplode(payload.hourly.relativehumidity_2m) AS (idx, relativehumidity_2m), 
                        payload.hourly_units.relativehumidity_2m AS unit_relativehumidity_2m
                    FROM clima_municipios
                )
                SELECT DISTINCT
                    _relativehumidity.idx,
                    _relativehumidity.codigo_ibge,
                    _relativehumidity.relativehumidity_2m,
                    _relativehumidity.unit_relativehumidity_2m
                FROM _relativehumidity
            """
        },
        {
            "dataset": "temperatura",
            "query": """
                WITH _temperature AS (
                    SELECT DISTINCT
                        codigo_ibge,
                        posexplode(payload.hourly.temperature_2m) AS (idx, temperature_2m),
                        payload.hourly_units.temperature_2m AS unit_temperature_2m
                    FROM clima_municipios
                )
                SELECT DISTINCT
                    _temperature.idx,
                    _temperature.codigo_ibge,
                    _temperature.temperature_2m,
                    _temperature.unit_temperature_2m
                FROM _temperature
            """
        },
        {
            "dataset": "velocidade_do_vento",
            "query": """
                WITH _windspeed AS (
                    SELECT DISTINCT
                        codigo_ibge,
                        posexplode(payload.hourly.windspeed_10m) AS (idx, windspeed_10m),
                        payload.hourly_units.windspeed_10m AS unit_windspeed_10m
                    FROM clima_municipios
                )
                SELECT DISTINCT
                    _windspeed.idx,
                    _windspeed.codigo_ibge,
                    _windspeed.windspeed_10m,
                    _windspeed.unit_windspeed_10m
                FROM _windspeed
            """
        },
    ]

    for dsqs in datasets_and_queries:
        dataset = dsqs["dataset"]
        query = dsqs["query"]        

        logging.info(f"DATA TRANSFORM - {dataset}")
        refined = spark.sql(query)

        logging.info(f"DATA WRITE - {dataset}")
        (
            refined.write
            .format("parquet")
            .mode("overwrite")
            .save(f"s3a://datalake/refined/clima_municipios_brasileiros/{dataset}/{reference_date.strftime('%Y')}")
        )

    if spark:
        spark.stop()

    return "SUCESSO"

@task()
def trusted(s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str, reference_date: datetime):
    from codes.utils_datalake import spark_session_builder

    spark = spark_session_builder(
        s3a_access_key=s3a_access_key, 
        s3a_secret_key=s3a_secret_key, 
        s3a_endpoint=s3a_endpoint, 
        app_name="trusted_clima_municipios_brasileiros"
    )

    spark.read.parquet(f"s3a://datalake/refined/clima_municipios_brasileiros/temporalidade/{reference_date.strftime('%Y')}") \
        .createOrReplaceTempView("temporalidade")
    spark.read.parquet(f"s3a://datalake/refined/clima_municipios_brasileiros/humidade/{reference_date.strftime('%Y')}") \
        .createOrReplaceTempView("humidade")
    spark.read.parquet(f"s3a://datalake/refined/clima_municipios_brasileiros/temperatura/{reference_date.strftime('%Y')}") \
        .createOrReplaceTempView("temperatura")
    spark.read.parquet(f"s3a://datalake/refined/clima_municipios_brasileiros/velocidade_do_vento/{reference_date.strftime('%Y')}") \
        .createOrReplaceTempView("velocidade_do_vento")

    datasets_and_queries = [
        {
            "dataset": "humidade",
            "query": """
                SELECT 
                    t.codigo_ibge,
                    t.`time` AS data_hora,
                    hm.relativehumidity_2m AS humidade_relativa,
                    -- hm.unit_relativehumidity_2m AS unidade_humidade_relativa,
                    current_timestamp() AS gerado_em
                FROM temporalidade t
                INNER JOIN humidade hm ON hm.idx = t.idx
                ORDER BY t.codigo_ibge
            """
        },
        {
            "dataset": "temperatura",
            "query": """
                SELECT 
                    t.codigo_ibge,
                    t.`time` AS data_hora,
                    tp.temperature_2m AS temperatura,
                    -- tp.unit_temperature_2m AS unidade_temperatura,
                    current_timestamp() AS gerado_em
                FROM temporalidade t
                INNER JOIN temperatura tp ON tp.idx = t.idx
                ORDER BY t.codigo_ibge
            """
        },
        {
            "dataset": "velocidade_do_vento",
            "query": """
                SELECT 
                    t.codigo_ibge,
                    t.`time` AS data_hora,
                    vv.windspeed_10m AS velocidade_do_vento,
                    -- vv.unit_windspeed_10m AS unidade_velocidade_do_vento,
                    current_timestamp() AS gerado_em
                FROM temporalidade t
                INNER JOIN velocidade_do_vento vv ON vv.idx = t.idx
                ORDER BY t.codigo_ibge
            """
        },
    ]

    for dsqs in datasets_and_queries:
        dataset = dsqs["dataset"]
        query = dsqs["query"]        

        logging.info(f"DATA TRANSFORM - {dataset}")
        trusted = spark.sql(query)
        
        logging.info("DATA WRITE")
        (
            trusted.write
            .format("parquet")
            .mode("overwrite")
            .save(f"s3a://datalake/trusted/clima_municipios_brasileiros/{dataset}/{reference_date.strftime('%Y')}")
        )

    if spark:
        spark.stop()