import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import col


load_dotenv('.env')

spark_app_name = os.getenv('SPARK_APP_NAME')
spark_master = os.getenv('SPARK_MASTER_URL')
spark = SparkSession.builder.appName(spark_app_name).master(spark_master).getOrCreate()


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", os.getenv('KAFKA_BOOTSTRAP_SERVERS')) \
  .option("subscribe", "positions") \
  .load()


work_df = df.select(col("key").cast("string"), col("value.*").cast("string"))

