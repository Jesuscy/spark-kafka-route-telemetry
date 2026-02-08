#!/bin/bash
set -e

SPARK_WORKLOAD=$1
echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

case "$SPARK_WORKLOAD" in
  master)
    exec start-master.sh \
      --host spark-master \
      --port 7077 \
      --webui-port 8080
    ;;
  worker)
    exec start-worker.sh \
      --host spark-worker \
      --port 7078 \
      --webui-port 8081 \
      spark://spark-master:7077
    ;;
  history)
    exec start-history-server.sh
    ;;
  *)
    echo "Unknown workload: $SPARK_WORKLOAD"
    exit 1
    ;;
esac
