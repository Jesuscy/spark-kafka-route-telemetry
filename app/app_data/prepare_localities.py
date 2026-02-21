import os
import io
import zipfile
import requests
import pandas as pd
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv('.env')

ADLS_STATIC_PATH = (
    f"abfss://trips-info-container-staging@{os.getenv("STORAGE_ACCOUNT_NAME")}.dfs.core.windows.net/static/"
    f"localities_spain.parquet"
)

GEONAMES_COLUMNS = [
    "geonameid", "name", "asciiname", "alternatenames",
    "lat", "lon", "feature_class", "feature_code",
    "country_code", "cc2", "admin1_code", "admin2_code",
    "admin3_code", "admin4_code", "population",
    "elevation", "dem", "timezone", "modification_date",
]

FEATURE_CODES_INTERES = {
    "PPL",   # Populated place
    "PPLA",  # Capital de provincia/estado
    "PPLA2", # Capital de segundo nivel
    "PPLA3", # Capital de tercer nivel
    "PPLC",  # Capital del país
    "PPLG",  # Seat of government
}

ADMIN1_ES = {
    "01": "Andalucía",
    "02": "Aragón",
    "03": "Asturias",
    "04": "Baleares",
    "05": "Canarias",
    "06": "Cantabria",
    "07": "Castilla-La Mancha",
    "08": "Castilla y León",
    "09": "Cataluña",
    "10": "Extremadura",
    "11": "Galicia",
    "12": "La Rioja",
    "13": "Madrid",
    "14": "Murcia",
    "15": "Navarra",
    "16": "País Vasco",
    "17": "Valencia",
    "18": "Ceuta",
    "19": "Melilla",
}


def download_admin2_mapping():
    #Me descargo un dataframe con información sobre las provincias y me quedo solo con las Españolas.
    print("Descargando nombres de provincias (admin2Codes)...")
    response = requests.get("https://download.geonames.org/export/dump/admin2Codes.txt", timeout=60)
    response.raise_for_status()

    mapping = {}
    for line in response.text.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0].startswith("ES."):
            code = parts[0]          
            name = parts[1].strip()  
            mapping[code] = name

    print(f"  Provincias encontradas: {len(mapping)}")
    return mapping


def download_localities(admin2_map: dict):

    response = requests.get("https://download.geonames.org/export/dump/ES.zip", timeout=120)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        with z.open("ES.txt") as f:
            df = pd.read_csv(
                f,
                sep="\t",
                header=None,
                names=GEONAMES_COLUMNS,
                dtype=str,
                low_memory=False,
            )

    print(f"  Registros totales: {len(df):,}")

    #Del todas las localidades me quedo solo con las interesadas, que son las capitales, ciudades grandes... y lo que hay comentado en FEATURE_CODES_INTERES
    df = df[
        (df["feature_class"] == "P") &
        (df["feature_code"].isin(FEATURE_CODES_INTERES))
    ].copy()

    
    print(f"  Localidades después de filtrar: {len(df):,}")

    #Convierto lat y lon a números, los que no se puedan pasar a número se borran.
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])

    #Del diccionario de provincias me creo una columna nueva con el nombre de la provincia, si está se pone como desconocida.
    df["province"] = df.apply(
        lambda row: admin2_map.get(f"ES.{row['admin2_code']}", "Desconocida"),
        axis=1,
    )
    #Del de comunidades lo mismo pero con la columna admin1_code.
    df["comunidad"] = df["admin1_code"].map(ADMIN1_ES).fillna("Desconocida")

    result = df[["name", "province", "comunidad", "lat", "lon"]].reset_index(drop=True)
    return result


def upload_to_adls(df: pd.DataFrame, spark: SparkSession):
    print(f"Subiendo a ADLS: {ADLS_STATIC_PATH}")
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("overwrite").parquet(ADLS_STATIC_PATH)
    print("Subida completada.")


def spark_connection():
    return (
        SparkSession.builder.appName("prepare_localities")
        .config(f"spark.hadoop.fs.azure.account.auth.type.{os.getenv("STORAGE_ACCOUNT_NAME")}.dfs.core.windows.net","OAuth",)
        .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{os.getenv("STORAGE_ACCOUNT_NAME")}.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",)
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{os.getenv("STORAGE_ACCOUNT_NAME")}.dfs.core.windows.net",os.getenv("CLIENT_ID"),)
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{os.getenv("STORAGE_ACCOUNT_NAME")}.dfs.core.windows.net",os.getenv("CLIENT_SECRET"),)
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{os.getenv("STORAGE_ACCOUNT_NAME")}.dfs.core.windows.net",f"https://login.microsoftonline.com/{os.getenv('TENANT_ID')}/oauth2/token",)
        .getOrCreate()
    )


if __name__ == "__main__":
    admin2_map = download_admin2_mapping()
    localities_df = download_localities(admin2_map)

    localities_df.to_parquet("localities_spain.parquet", index=False)
    print(f"Parquet local guardado: {"localities_spain.parquet"}")
    print(localities_df.head(5).to_string())

    spark = spark_connection()
    upload_to_adls(localities_df, spark)
    spark.stop()