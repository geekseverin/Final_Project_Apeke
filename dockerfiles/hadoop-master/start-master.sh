#!/bin/bash

set -e

echo "Starting Hadoop Master Node..."

# Copier les configurations
cp /config/hadoop/* $HADOOP_CONF_DIR/
cp /config/spark/* $SPARK_CONF_DIR/ 2>/dev/null || true

# Démarrer SSH
#sudo service ssh start

# Initialiser Spark
/scripts/setup/init-spark.sh

# Formater HDFS si nécessaire
if [ ! -d "/hadoop/data/namenode/current" ]; then
    echo "Formatting HDFS..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
fi

# Démarrer les services Hadoop
echo "Starting NameNode..."
$HADOOP_HOME/bin/hdfs --daemon start namenode

echo "Starting ResourceManager..."
$HADOOP_HOME/bin/yarn --daemon start resourcemanager

# Attendre que NameNode soit prêt
sleep 15

# Créer les répertoires HDFS de base
echo "Creating HDFS directories..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hadoop || true
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-logs || true
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /pig-data || true
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /pig-output || true
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-output || true

# Démarrer Spark Master
echo "Starting Spark Master..."
$SPARK_HOME/sbin/start-master.sh

# Démarrer History Server
$SPARK_HOME/sbin/start-history-server.sh || true

echo "Hadoop Master node started successfully!"
echo "Services available:"
echo "  - NameNode Web UI: http://localhost:9870"
echo "  - ResourceManager Web UI: http://localhost:8088"
echo "  - Spark Master Web UI: http://localhost:8080"

# Maintenir le conteneur en vie
tail -f $HADOOP_HOME/logs/* $SPARK_HOME/logs/* 2>/dev/null || tail -f /dev/null