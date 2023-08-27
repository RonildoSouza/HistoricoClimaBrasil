import logging

def spark_session_builder(s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str, app_name: str):
    from pyspark.sql import SparkSession

    logging.info("CREATE SPARK SESSION")

    spark = (
        SparkSession.builder
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1,com.amazonaws:aws-java-sdk-bundle:1.12.469,org.apache.hadoop:hadoop-aws:3.3.4")

        # CONFIGURA EXTENSÃO DO DELTA
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # LIMITA USO DE CORE E MEMORIA POR EXECUTOR
        .config("spark.cores.max", "1")
        .config("spark.executor.memory", "2g")  

        # CONFIGURAÇÃO PARA COMUNICAR COM PROTOCOLO S3
        .config("spark.hadoop.fs.s3a.access.key", s3a_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3a_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{s3a_endpoint}")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .config("spark.network.timeout", "480s")

        # CRIA SESSÃO DO SPARK
        .master(f"spark://spark-master:7077")
        .appName(app_name)
        .getOrCreate()
    )

    # hadoop_base_path = os.getenv("HADOOP_HOME").replace("\\", "/")
    # hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
    # hadoop_config.set("driver.extraClassPath", f"{hadoop_base_path}/lib/native/hadoop-aws-3.3.1.jar:{hadoop_base_path}/lib/native/aws-java-sdk-1.12.153")

    # for key, value in [(k.replace("spark.hadoop.", ""), v) for k, v in spark.sparkContext.getConf().getAll() if k.find("hadoop") != -1]:
    #     hadoop_config.set(key, value)

    spark.sparkContext.setLogLevel("OFF")

    return spark

def upload_file(bucket_prefix: str, filename_with_extension: str, filebytes: bytes, s3a_access_key: str, s3a_secret_key: str, s3a_endpoint: str):
    import gzip
    import boto3 

    BUCKET = 'datalake'
    
    s3 = boto3.resource(
        's3', 
        aws_access_key_id=s3a_access_key,
        aws_secret_access_key=s3a_secret_key,
        endpoint_url=f"http://{s3a_endpoint}", 
        use_ssl=False
    )

    logging.info("UPLOAD FILE TO S3")
    (
        s3.Object(BUCKET, f'{bucket_prefix}/{filename_with_extension}.gz')
        .put(Body=gzip.compress(filebytes))
    )