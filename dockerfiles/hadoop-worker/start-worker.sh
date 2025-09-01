#!/bin/bash

set -e

echo "Starting Hadoop Worker Node..."

# Copier les configurations
cp /config/hadoop/* $HADOOP_CONF_DIR/
cp /config/spark/* $SPARK_CONF_DIR/ 2>/dev/null || true

# Démarrer SSH
#service ssh start

# Initialiser Spark
/scripts/setup/init-spark.sh

# Fonction pour attendre qu'un service soit prêt
wait_for_service() {
    local host=$1
    local port=$2
    local service=$3
    
    echo "Waiting for $service on $host:$port..."
    while ! nc -z $host $port 2>/dev/null; do
        sleep 2
    done
    echo "$service is ready!"
}

# Attendre que le master soit prêt
wait_for_service hadoop-master 9870 "NameNode"
wait_for_service hadoop-master 8088 "ResourceManager"
wait_for_service hadoop-master 7077 "Spark Master"

# Démarrer DataNode
echo "Starting DataNode..."
$HADOOP_HOME/bin/hdfs --daemon start datanode

# Démarrer NodeManager
echo "Starting NodeManager..."
$HADOOP_HOME/bin/yarn --daemon start nodemanager

# Calculer le port Spark Worker basé sur WORKER_ID
SPARK_WORKER_PORT=$((8080 + ${WORKER_ID:-1}))

# Démarrer Spark Worker
echo "Starting Spark Worker on port $SPARK_WORKER_PORT..."
$SPARK_HOME/sbin/start-worker.sh spark://hadoop-master:7077 \
    --webui-port $SPARK_WORKER_PORT

echo "Hadoop Worker node started successfully!"
echo "  - DataNode connected to NameNode"
echo "  - NodeManager connected to ResourceManager"
echo "  - Spark Worker connected to Master"

# Maintenir le conteneur en vie
tail -f $HADOOP_HOME/logs/* $SPARK_HOME/logs/* 2>/dev/null || tail -f /dev/null