import os
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv

load_dotenv('.env')


def spark_connection():
    spark_app_name = os.getenv('SPARK_APP_NAME')
    spark = SparkSession.builder.appName(spark_app_name) \
        .config(f"spark.hadoop.fs.azure.account.auth.type.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", "OAuth") \
        .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", os.getenv("CLIENT_ID")) \
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", os.getenv("CLIENT_SECRET")) \
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}/oauth2/token") \
        .getOrCreate()

    return spark


def load_localities_broadcast(spark):
    #Cargo el parquet de localidades y lo paso a diccionario.
    localities_pd = spark.read.parquet(
        f"abfss://trips-info-container-staging@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/static/localities_spain.parquet"
    ).toPandas()
    return spark.sparkContext.broadcast(localities_pd.to_dict('records'))


def make_locality_udf(localities_bc):
    #UDF para calcular la localidad más cercana a unas coordenadas dadas, usando la fórmula de Haversine para calcular la distancia entre dos puntos geográficos. 
    def get_locality(lat, lon):
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            min_dist = float("inf")
            result = "Desconocido"

            for row in localities_bc.value:
                dlat = math.radians(lat_f - row['lat'])
                dlon = math.radians(lon_f - row['lon'])
                a = (math.sin(dlat / 2) ** 2
                     + math.cos(math.radians(lat_f))
                     * math.cos(math.radians(row['lat']))
                     * math.sin(dlon / 2) ** 2)
                dist = 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

                if dist < min_dist:
                    min_dist = dist
                    result = f"{row['name']}, {row['province']}"

            return result
        except Exception:
            return "Desconocido"

    return udf(get_locality, StringType())


def spark_streaming():
    spark = spark_connection()

    schema = StructType([
        StructField("key", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("model", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("data_timestamp", StringType(), True),
        StructField("progress", StringType(), True),
    ])

    localities_bc = load_localities_broadcast(spark)
    locality_udf = make_locality_udf(localities_bc)

    df = spark \
        .readStream \
        .format("parquet") \
        .schema(schema) \
        .option("path", f"abfss://trips-info-container-landing@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/data/") \
        .load()

    enriched_df = df \
        .withColumnRenamed("key", "trip_id") \
        .withColumn("localidad_actual", locality_udf(col("latitude"), col("longitude")))

    query = enriched_df.writeStream \
        .format("parquet") \
        .partitionBy("vehicle_id") \
        .option("path", f"abfss://trips-info-container-staging@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/data/") \
        .option("checkpointLocation", f"abfss://trips-info-container-staging@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/checkpoints/staging") \
        .outputMode("append") \
        .start()

    query.awaitTermination()


spark_streaming()
