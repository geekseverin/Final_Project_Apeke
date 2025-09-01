FROM mongo:6.0

# Copier le script d'initialisation
COPY init-mongo.js /docker-entrypoint-initdb.d/

# Installer mongoimport
RUN apt-get update && apt-get install -y mongodb-database-tools && rm -rf /var/lib/apt/lists/*

# Créer le répertoire pour les données d'exemple
RUN mkdir -p /sample-data

# Copier les données d'exemple
COPY sample-data/ /sample-data/

EXPOSE 27017