FROM mongo:6.0

# Installer les outils nécessaires
RUN apt-get update && apt-get install -y \
    mongodb-database-tools \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Installer pymongo pour les scripts Python
RUN pip3 install pymongo

# Copier le script d'initialisation
COPY init-mongo.js /docker-entrypoint-initdb.d/

# Créer le répertoire pour les données d'exemple
RUN mkdir -p /sample-data

# Copier les données d'exemple
COPY sample-data/ /sample-data/

# Rendre le script de population exécutable
COPY populate_mongodb.sh /scripts/setup/populate_mongodb.sh
RUN chmod +x /scripts/setup/populate_mongodb.sh

# Exposer le port MongoDB
EXPOSE 27017

# Commande par défaut (héritée de mongo:6.0)
CMD ["mongod", "--bind_ip_all"]