#!/bin/bash

set -e

ROLE=${1:-worker}

echo "Initializing Hadoop node with role: $ROLE"

# Détecter JAVA_HOME automatiquement si nécessaire
if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
    echo "JAVA_HOME détecté: $JAVA_HOME"
fi

# Copier les configurations
cp /config/hadoop/* $HADOOP_CONF_DIR/
cp /config/spark/* $SPARK_CONF_DIR/

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

case $ROLE in
    "master")
        echo "Starting Hadoop Master (NameNode + ResourceManager + Spark Master)"
        
        # Initialiser Spark d'abord
        /scripts/setup/init-spark.sh
        
        # Formater HDFS si nécessaire
        if [ ! -d "/hadoop/data/namenode/current" ]; then
            echo "Formatting HDFS..."
            $HADOOP_HOME/bin/hdfs namenode -format -force
        fi
        
        # Démarrer NameNode
        echo "Starting NameNode..."
        $HADOOP_HOME/bin/hdfs --daemon start namenode
        
        # Démarrer ResourceManager
        echo "Starting ResourceManager..."
        $HADOOP_HOME/bin/yarn --daemon start resourcemanager
        
        # Attendre un peu
        sleep 10
        
        # Créer les répertoires nécessaires dans HDFS
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hadoop || true
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-logs || true
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p /pig-data || true
        
        # Démarrer Spark Master
        echo "Starting Spark Master..."
        $SPARK_HOME/sbin/start-master.sh
        
        echo "Master node started successfully"
        ;;
        
    "secondary")
        echo "Starting Secondary NameNode"
        
        # Attendre que le master soit prêt
        wait_for_service hadoop-master 9870 "NameNode"
        
        # Démarrer Secondary NameNode
        $HADOOP_HOME/bin/hdfs --daemon start secondarynamenode
        
        echo "Secondary NameNode started successfully"
        ;;
        
    "worker")
        echo "Starting Hadoop Worker (DataNode + NodeManager + Spark Worker)"
        
        # Initialiser Spark
        /scripts/setup/init-spark.sh
        
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
        
        # Démarrer Spark Worker
        echo "Starting Spark Worker..."
        $SPARK_HOME/sbin/start-worker.sh spark://hadoop-master:7077
        
        echo "Worker node started successfully"
        ;;
esac

# Maintenir le conteneur en vie
tail -f /dev/null