#!/bin/bash

set -e

ROLE=${1:-worker}

echo "Initializing Hadoop node with role: $ROLE"

# Copier les configurations
cp /config/hadoop/* $HADOOP_CONF_DIR/
cp /config/spark/* $SPARK_CONF_DIR/

# Démarrer SSH
service ssh start

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
        
        # Formater HDFS si nécessaire
        if [ ! -d "/hadoop/data/namenode/current" ]; then
            echo "Formatting HDFS..."
            $HADOOP_HOME/bin/hdfs namenode -format -force
        fi
        
        # Démarrer NameNode
        $HADOOP_HOME/bin/hdfs --daemon start namenode
        
        # Démarrer ResourceManager
        $HADOOP_HOME/sbin/start-yarn.sh
        
        # Créer les répertoires nécessaires dans HDFS
        sleep 10
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hadoop
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p /spark-logs
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p /pig-data
        
        # Démarrer Spark Master
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
        
        # Attendre que le master soit prêt
        wait_for_service hadoop-master 9870 "NameNode"
        wait_for_service hadoop-master 8088 "ResourceManager"
        wait_for_service hadoop-master 7077 "Spark Master"
        
        # Démarrer DataNode
        $HADOOP_HOME/bin/hdfs --daemon start datanode
        
        # Démarrer NodeManager
        $HADOOP_HOME/bin/yarn --daemon start nodemanager
        
        # Démarrer Spark Worker
        $SPARK_HOME/sbin/start-worker.sh spark://hadoop-master:7077
        
        echo "Worker node started successfully"
        ;;
esac

# Maintenir le conteneur en vie
tail -f /dev/null