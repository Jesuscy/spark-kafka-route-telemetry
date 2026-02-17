# Proyecto de Simulación de Viajes y Procesamiento de Datos en Streaming con Kafka y Spark
[ESTE PROYECTO ESTÁ EN DESARROLLO, LO ÚLTIMO QUE TENGO PENSADO IMPLEMENTAR ES LA WEB APP.]

Este proyecto está enfocado en la **generación de datos simulados** y su **procesamiento en streaming** utilizando **Apache Kafka** y **Apache Spark**.
La idea principal es contar con una **Web App** que permita la creación de vehículos y la definición de viajes por parte del usuario.

## Descripción General
- Los vehículos creados simulan recorrer una ruta definida por el usuario.
- Durante el recorrido, cada vehículo **reporta su posición y estado del viaje** en tiempo real.
- Estos datos se envían mediante **Kafka** y son procesados por **Spark** para su posterior almacenamiento y análisis.

## Stack Tecnológico
| Componente | Tecnología | Uso |
|---|---|---|
| Simulación de rutas | **OSRM API** (Open Source Routing Machine) | Cálculo de rutas reales entre coordenadas |
| Mensajería | **Apache Kafka** 8.0.0 (Confluent, KRaft) | Streaming de eventos de posición |
| Procesamiento | **Apache Spark** 3.3.1 + **PySpark** | Lectura streaming desde Kafka y escritura en el Data Lake |
| Almacenamiento | **Azure Data Lake Storage Gen2** | Repositorio de datos en capas (landing, staging, common) |
| Orquestación | **Apache Airflow** 3.1.2 (CeleryExecutor) | Transformaciones programadas entre capas del Data Lake |
| Infraestructura | **Docker** + **Docker Compose** | Despliegue local de todos los servicios |
| Monitorización | **Conduktor Console**, **Spark History Server** | UIs para Kafka y Spark respectivamente |

## Arquitectura
Para evitar los altos costos del procesamiento de datos en streaming en la nube, se ha optado por un despliegue **local**:

- **Kafka**, **Spark** y **Airflow** se ejecutan en contenedores Docker, configurados para trabajar en la misma red (`infra_net`) y comunicarse entre sí.
- La única parte desplegada en la nube es un **Azure Data Lake**, que actúa como repositorio final donde Spark deja los datos procesados.
- Este Data Lake permite que, en el futuro, se pueda añadir una capa de **reporting** o visualización sobre los datos ya procesados.

## Flujo de Datos
1. El usuario crea un vehículo y define un viaje (origen, destino, ruta).
2. La ruta se calcula mediante la **API de OSRM**, que devuelve las coordenadas reales del recorrido.
3. El vehículo simula el recorrido, enviando eventos de posición y estado al topic `vehicle_positions` de Kafka.
4. Spark consume estos eventos en streaming, los parsea y los escribe en formato **Parquet** en el Azure Data Lake (capa landing), particionados por `vehicle_id`.
5. **Airflow** ejecuta transformaciones programadas para mover los datos de **landing** a **staging** y de **staging** a **common**.
6. Los datos quedan listos para ser utilizados en análisis posteriores o integrados en dashboards.

<img width="1185" height="578" alt="image" src="https://github.com/user-attachments/assets/1e8dc6b0-dea1-4754-a3a3-8bd6fb719de5" />

## Modelo de Datos
Cada evento enviado a Kafka contiene la siguiente estructura:

| Campo | Tipo | Descripción |
|---|---|---|
| `vehicle_id` | String | Identificador del vehículo |
| `model` | String | Modelo del vehículo |
| `latitude` | String | Latitud de la posición actual |
| `longitude` | String | Longitud de la posición actual |
| `data_timestamp` | String | Fecha y hora del evento (`YYYY-MM-DD HH:MM:SS`) |
| `progress` | String | Progreso del viaje (ej: `45%`) |

## Estructura del Proyecto
```
├── app/
│   ├── app_core/                # Lógica de negocio
│   │   ├── Main.py              # Orquestación de viajes y envío concurrente
│   │   ├── Producer.py          # Productor Kafka (confluent_kafka)
│   │   ├── Trip.py              # Simulación del recorrido
│   │   ├── Route.py             # Peticiones a la API de OSRM
│   │   └── Vehicle.py           # Modelo de vehículo
│   ├── app_data/
│   │   ├── landing_data.py      # Job Spark Streaming (Kafka → Azure Data Lake)
│   │   └── airflow_dags/        # DAGs de Airflow
│   │       ├── landing_to_staging.py
│   │       └── staging_to_common.py
│   └── app_utils/
│       └── azure_uploader.py    # Utilidad legacy de subida a Azure
├── infra/
│   ├── kafka/
│   │   ├── docker-compose.kafka.yml  # Kafka + Conduktor + PostgreSQL
│   │   └── .env
│   ├── spark/
│   │   ├── Dockerfile           # Imagen Spark con JARs de Azure y Kafka
│   │   ├── docker-compose.yml   # Spark Master + Workers + History Server
│   │   ├── Makefile             # Comandos de build, run y submit
│   │   ├── entrypoint.sh
│   │   ├── requirements.txt
│   │   └── conf/spark-defaults.conf
│   └── airflow/
│       ├── Dockerfile
│       ├── docker-compose.yaml  # Airflow (Scheduler, Worker, API Server, Redis, PostgreSQL)
│       └── config/airflow.cfg
├── .env                         # Variables de entorno (Azure, Kafka, API)
└── README.md
```

## Requisitos Previos
- **Docker** y **Docker Compose**
- Una **Azure Storage Account** con Data Lake Storage Gen2 habilitado
- Tres contenedores en la Storage Account: `trips-info-container-landing`, `trips-info-container-staging`, `trips-info-container-common`
- Un **App Registration** en Azure AD con rol de **Storage Blob Data Contributor** y permisos rwx sobre los contenedores

## Configuración
Crear un fichero `.env` en la raíz del proyecto con las siguientes variables:

```env
API_URL=https://router.project-osrm.org/route/v1/driving/
STORAGE_ACCOUNT_NAME=<nombre_storage_account>
CLIENT_ID=<client_id_del_app_registration>
CLIENT_SECRET=<client_secret>
TENANT_ID=<tenant_id>
KAFKA_BOOTSTRAP_SERVERS=kafka1:19092
```

## Cómo Levantar el Proyecto

### 1. Crear la red Docker compartida
```bash
docker network create infra_net
```

### 2. Levantar Kafka
```bash
cd infra/kafka
docker compose -f docker-compose.kafka.yml up -d
```
La UI de **Conduktor** estará disponible en `http://localhost:8080`.

### 3. Levantar Spark
```bash
cd infra/spark
make build
make run-d
```
- **Spark Master UI**: `http://localhost:9090`
- **Spark History Server**: `http://localhost:18080`

### 4. Lanzar el job de Spark Streaming
```bash
make submit
```
Esto ejecuta `spark-submit` dentro del contenedor master con el script `landing_data.py`.

### 5. Levantar Airflow (opcional)
```bash
cd infra/airflow
docker compose up -d
```
La UI de **Airflow** estará disponible en `http://localhost:8080`.

### 6. Ejecutar la simulación de viajes
```bash
python app/app_core/Main.py
```

## Cuaderno de desarrollo:
- Desarrollo scripts, realizar la petición y simular el recorrido de la ruta.
- Creación de las imagenes de los servicios de **Kafka** y **Spark** en **Docker**, desarrollo de un Dockerfile para spark, instalando las dependencias de **Azure** y **Kafka**.
- Creación de una Storage Account y contenedores landing, staging, common.
- Creación de un **App Registration** con rol de Storage Account Contributor y permisos rwx sobre el contenedor de landing.
- Desarrollo script **PySpark** conectado al topic de Kafka, realizando lectura, procesamiento y escritura streaming de datos sobre el contenedor de landing autorizado mediante **service principal**.
- Despliegue de **Airflow** con CeleryExecutor (Scheduler, Worker, API Server) conectado a la misma red Docker que Kafka y Spark.
- Creación de DAGs base para las transformaciones **landing → staging** y **staging → common**.
- En la versión Web App desarrollo de **endpoints** con **FastAPI**.

## Por desarrollar
- Web Frontend, WebSocket.
- Implementar la lógica de los DAGs de **Airflow** para las transformaciones de datos entre capas del Data Lake (landing → staging → common).
