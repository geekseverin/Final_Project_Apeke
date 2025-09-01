#!/bin/bash

# Script d'initialisation pour Apache Spark
# Projet Big Data - Traitement Distribué 2024-2025

set -e

echo "Initialisation d'Apache Spark..."

# Détecter JAVA_HOME automatiquement si nécessaire
if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
    echo "JAVA_HOME détecté automatiquement: $JAVA_HOME"
fi

# Variables d'environnement
export SPARK_HOME=/opt/spark
export HADOOP_HOME=/opt/hadoop
export SPARK_CONF_DIR=$SPARK_HOME/conf

# Créer le répertoire de configuration s'il n'existe pas
mkdir -p $SPARK_CONF_DIR

# Copier les configurations
if [ -f "/config/spark/spark-defaults.conf" ]; then
    cp /config/spark/spark-defaults.conf $SPARK_CONF_DIR/
    echo "Configuration Spark copiée"
fi

# Créer spark-env.sh avec le bon JAVA_HOME
cat > $SPARK_CONF_DIR/spark-env.sh << EOF
#!/usr/bin/env bash

# Java Home
export JAVA_HOME=$JAVA_HOME

# Hadoop Configuration
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=\$(\$HADOOP_HOME/bin/hadoop classpath)

# Spark Configuration
export SPARK_MASTER_HOST=hadoop-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_WEBUI_PORT=8081

# Memory settings
export SPARK_WORKER_MEMORY=1g
export SPARK_DRIVER_MEMORY=1g
export SPARK_EXECUTOR_MEMORY=1g

# MongoDB Integration
export SPARK_CLASSPATH=\$SPARK_CLASSPATH:/opt/hadoop/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar
export SPARK_CLASSPATH=\$SPARK_CLASSPATH:/opt/hadoop/share/hadoop/common/lib/mongodb-driver-3.12.11.jar
EOF

chmod +x $SPARK_CONF_DIR/spark-env.sh

# Créer le fichier slaves/workers
echo "hadoop-worker1" > $SPARK_CONF_DIR/slaves
echo "hadoop-worker2" >> $SPARK_CONF_DIR/slaves
echo "hadoop-worker3" >> $SPARK_CONF_DIR/slaves

# S'assurer que les répertoires de logs existent
mkdir -p $SPARK_HOME/logs

echo "Spark initialisé avec succès!"