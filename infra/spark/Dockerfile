FROM python:3.11-bullseye as spark-abse

ARG SPARK_VERSION=3.4.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \
    curl \
    vim \
    unzip \
    rsync \
    openjdk-11-jdk \
    build-essential \
    software-properties-common \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*do

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

RUN curl -fL https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz \
    -o spark.tgz \
 && tar xvzf spark.tgz --directory /opt/spark --strip-components 1 \
 && rm -f spark.tgz

    #Dependencias Kafka
RUN curl -fL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.1/spark-sql-kafka-0-10_2.12-3.3.1.jar \
    -o $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.3.1.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.1/spark-token-provider-kafka-0-10_2.12-3.3.1.jar \
    -o $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.3.1.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar \
    -o $SPARK_HOME/jars/kafka-clients-3.2.0.jar

    # Dependencias Azure ADLS Gen2 
RUN curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar \
    -o $SPARK_HOME/jars/hadoop-azure-3.3.1.jar && \
    curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.1/hadoop-common-3.3.1.jar \
    -o $SPARK_HOME/jars/hadoop-common-3.3.1.jar && \
    curl -fL https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.14.2/azure-storage-blob-12.14.2.jar \
    -o $SPARK_HOME/jars/azure-storage-blob-12.14.2.jar && \
    curl -fL https://repo1.maven.org/maven2/com/azure/azure-core/1.29.1/azure-core-1.29.1.jar \
    -o $SPARK_HOME/jars/azure-core-1.29.1.jar


COPY infra/spark/requirements.txt . 
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

COPY infra/spark/conf/spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

RUN mkdir -p /opt/spark/spark-events

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY infra/spark/entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

COPY app/app_data/landing_data.py /opt/spark/jobs/streaming_job.py

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
