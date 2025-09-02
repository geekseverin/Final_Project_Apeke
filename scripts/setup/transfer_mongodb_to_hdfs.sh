#!/bin/bash

# Script pour transférer les données MongoDB vers HDFS
# Projet Big Data - Traitement Distribué 2024-2025

set -e

echo "🔄 Transfert des données MongoDB vers HDFS..."

# Variables
MONGODB_HOST="mongodb:27017"
HDFS_BASE="hdfs://hadoop-master:9000"
TEMP_DIR="/tmp/mongo_export"

# Créer le répertoire temporaire
mkdir -p $TEMP_DIR

# Fonction pour exporter une collection MongoDB vers HDFS
export_collection() {
    local collection=$1
    local database=$2
    
    echo "[INFO] Export de la collection $collection..."
    
    # Export MongoDB vers JSON avec mongoexport depuis le conteneur MongoDB
    docker exec mongodb mongoexport --host localhost:27017 \
        --username admin \
        --password password123 \
        --authenticationDatabase admin \
        --db $database \
        --collection $collection \
        --out /tmp/${collection}.json \
        --jsonArray
    
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] Collection $collection exportée dans le conteneur MongoDB"
        
        # Copier le fichier du conteneur MongoDB vers le conteneur Hadoop
        docker cp mongodb:/tmp/${collection}.json $TEMP_DIR/${collection}.json
        
        # Vérifier si le fichier existe et n'est pas vide
        if [ -s "$TEMP_DIR/${collection}.json" ]; then
            # Copier vers HDFS
            hdfs dfs -put -f $TEMP_DIR/${collection}.json $HDFS_BASE/data/
            
            if [ $? -eq 0 ]; then
                echo "[SUCCESS] Collection $collection copiée vers HDFS"
                
                # Vérifier dans HDFS
                hdfs dfs -ls $HDFS_BASE/data/${collection}.json
            else
                echo "[ERROR] Échec de la copie vers HDFS pour $collection"
                return 1
            fi
        else
            echo "[ERROR] Le fichier exporté $collection.json est vide ou n'existe pas"
            return 1
        fi
        
        # Nettoyer le fichier temporaire dans MongoDB
        docker exec mongodb rm -f /tmp/${collection}.json
        
    else
        echo "[ERROR] Échec de l'export pour $collection"
        return 1
    fi
}

# Créer les répertoires HDFS s'ils n'existent pas
echo "[INFO] Création des répertoires HDFS..."
hdfs dfs -mkdir -p $HDFS_BASE/data
hdfs dfs -mkdir -p $HDFS_BASE/pig-output

# Exporter les collections existantes
echo "[INFO] Début de l'export des collections..."
export_collection "sales" "bigdata"
export_collection "customers" "bigdata"

# Nettoyer les fichiers temporaires
echo "[INFO] Nettoyage des fichiers temporaires..."
rm -rf $TEMP_DIR

echo "[SUCCESS] Transfert MongoDB vers HDFS terminé!"

# Vérifier les données dans HDFS
echo "[INFO] Contenu de HDFS /data :"
hdfs dfs -ls $HDFS_BASE/data/

# Afficher un aperçu des données
echo "[INFO] Aperçu des données sales dans HDFS :"
hdfs dfs -head $HDFS_BASE/data/sales.json

echo "✅ Transfert terminé avec succès!"