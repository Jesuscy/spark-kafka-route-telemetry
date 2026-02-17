import os
from numpy import info
from requests_cache import datetime, logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv


#Script de procesamiento de datos con Spark, este es el que se incluye en el contenedor de Spark en /opt/spark/jobs/ .

load_dotenv('.env')


def spark_conection():

  spark_app_name = os.getenv('SPARK_APP_NAME')
  spark_master = os.getenv('SPARK_MASTER_URL')
  spark = SparkSession.builder.appName(spark_app_name) \
  .config(f"spark.hadoop.fs.azure.account.auth.type.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", "OAuth") \
  .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
  .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", os.getenv("CLIENT_ID")) \
  .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", os.getenv("CLIENT_SECRET")) \
  .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net",  f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}/oauth2/token") \
    .getOrCreate()
  
  return spark

def streaming_writing():
  spark = spark_conection()

  df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv('KAFKA_BOOTSTRAP_SERVERS')) \
    .option("subscribe", "vehicle_positions") \
    .option("startingOffsets", "earliest") \
    .load()

  schema = StructType([
      StructField("id", StringType(), True),
      StructField("lat", StringType(), True),
      StructField("lon", StringType(), True),
      StructField("timestamp", StringType(), True)
  ])


  parsed_df = df.select(col("key").cast("string"),     
                      from_json(col("value").cast("string"), schema).alias("data")
                      ).select("key", "data.*")
    
  query = parsed_df.writeStream \
      .format("parquet") \
      .partitionBy("id","timestamp") \
      .option("path", f"abfss://trips-info-container-landing@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/data/") \
      .option("checkpointLocation", f"abfss://trips-info-container-landing@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/checkpoints/positions") \
      .outputMode("append") \
      .start()
      
    
  query.awaitTermination()

 
streaming_writing()

