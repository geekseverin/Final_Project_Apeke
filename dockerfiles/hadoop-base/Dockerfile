FROM openjdk:8-jdk

# Installer les dépendances
RUN apt-get update && apt-get install -y \
    ssh \
    rsync \
    curl \
    wget \
    vim \
    python3 \
    python3-pip \
    netcat \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Variables d'environnement

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_VERSION=3.3.4
ENV SPARK_VERSION=3.4.1
ENV PIG_VERSION=0.17.0


# AJOUTER CETTE LIGNE pour créer le répertoire SSH
RUN mkdir -p /run/sshd

# Créer utilisateur hadoop
RUN useradd -m -s /bin/bash hadoop && \
    echo "hadoop:hadoop" | chpasswd && \
    adduser hadoop sudo

# Télécharger et installer Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    chown -R hadoop:hadoop /opt/hadoop

# Télécharger et installer Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    chown -R hadoop:hadoop /opt/spark

# Télécharger et installer Pig
RUN wget https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz && \
    mv pig-${PIG_VERSION} /opt/pig && \
    rm pig-${PIG_VERSION}.tar.gz && \
    chown -R hadoop:hadoop /opt/pig

# Configurer les variables d'environnement
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PIG_HOME=/opt/pig
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV SPARK_CONF_DIR=${SPARK_HOME}/conf
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PIG_HOME}/bin

# Créer les répertoires de données
RUN mkdir -p /hadoop/data/namenode && \
    mkdir -p /hadoop/data/datanode && \
    mkdir -p /hadoop/data/secondary && \
    chown -R hadoop:hadoop /hadoop

# Télécharger MongoDB Hadoop Connector
RUN wget https://repo1.maven.org/maven2/org/mongodb/mongo-hadoop/mongo-hadoop-core/2.0.2/mongo-hadoop-core-2.0.2.jar -O /opt/hadoop/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar && \
    wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.12.11/mongodb-driver-3.12.11.jar -O /opt/hadoop/share/hadoop/common/lib/mongodb-driver-3.12.11.jar

# Passer à l'utilisateur hadoop
USER hadoop
WORKDIR /home/hadoop

# Créer le répertoire .ssh et configurer SSH pour l'utilisateur hadoop
RUN mkdir -p ~/.ssh && \
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    chmod 700 ~/.ssh

# Configuration SSH
RUN echo "StrictHostKeyChecking no" >> ~/.ssh/config && \
    echo "UserKnownHostsFile /dev/null" >> ~/.ssh/config && \
    chmod 600 ~/.ssh/config

EXPOSE 9870 8088 8080 7077