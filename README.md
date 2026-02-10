# Proyecto de Simulación de Viajes y Procesamiento de Datos en Streaming con Kafka y Spark
[ESTE PROYECTO ESTÁ EN DESARROLLO, LO ÚLTIMO QUE TENGO PENSADO IMPLEMENTAR ES LA WEB APP.]

Este proyecto está enfocado en la **generación de datos simulados** y su **procesamiento en streaming** utilizando **Apache Kafka** y **Apache Spark**.  
La idea principal es contar con una **Web App** que permita la creación de vehículos y la definición de viajes por parte del usuario.

## Descripción General
- Los vehículos creados simulan recorrer una ruta definida por el usuario.
- Durante el recorrido, cada vehículo **reporta su posición y estado del viaje** en tiempo real.
- Estos datos se envían mediante **Kafka** y son procesados por **Spark** para su posterior almacenamiento y análisis.

## Arquitectura
Para evitar los altos costos del procesamiento de datos en streaming en la nube, se ha optado por un despliegue **local**:

- **Kafka** y **Spark** se ejecutan en contenedores Docker, configurados para trabajar en la misma red y comunicarse entre sí.
- La única parte desplegada en la nube es un **Azure Data Lake**, que actúa como repositorio final donde Spark deja los datos procesados.
- Este Data Lake permite que, en el futuro, se pueda añadir una capa de **reporting** o visualización sobre los datos ya procesados.

## Flujo de Datos
1. El usuario crea un vehículo y define un viaje (origen, destino, ruta).
2. El vehículo simula el recorrido, enviando eventos de posición y estado a Kafka.
3. Spark consume estos eventos en streaming, los procesa y los guarda en el Azure Data Lake.
4. Los datos quedan listos para ser utilizados en análisis posteriores o integrados en dashboards.

<img width="1185" height="578" alt="image" src="https://github.com/user-attachments/assets/1e8dc6b0-dea1-4754-a3a3-8bd6fb719de5" />

## Cuaderno de desarrollo:
- Desarrollo scripts, realizar la petición y simular el recorrido de la ruta.
- Creación de las imagenes de los servicios de **Kafka** y **Spark** en **Docker**, desarrollo de un Dockerfile para spark, instalando las dependencias de **Azure** y **Kafka**.
- Creación de una Storage Account y contendores langing, staging, common.
- Creación de un **App Registration** con rol de Storage Account Contributor permisos rwx sobre el contendor de landing.
- Desarrollo script **PySpark** conectado al topic de Kafka, realizando lectura, procesamiento y escritura streaming  de datos sobre el contenedor de laging autorizado mediante **service principal**.
- En la versión Web App desarrollo de **endpoints** con **FastAPI**.

## Por desarrollar
- Web Frontend, WebSocket.
- Desplegar contenedor con imagen de **Airflow** que diariamente ejecute un procesamiento de datos llevando datos de **langing** a **staging** y **common**.
