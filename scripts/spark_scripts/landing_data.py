import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv('.env')

spark_app_name = os.getenv('SPARK_APP_NAME')
spark_master = os.getenv('SPARK_MASTER_URL')
spark = SparkSession.builder.appName(spark_app_name).master(spark_master).getOrCreate()