#!/bin/bash

# Script de population MongoDB avec des données d'exemple
# Projet Big Data - Traitement Distribué 2024-2025

set -e

echo "Population de MongoDB avec des données d'exemple..."

# Attendre que MongoDB soit prêt
sleep 10

# Importer les données de ventes
mongoimport --host mongodb:27017 \
    --username admin \
    --password password123 \
    --authenticationDatabase admin \
    --db bigdata \
    --collection sales \
    --file /sample-data/sales.json \
    --jsonArray \
    --drop

echo "Données de ventes importées"

# Importer les données clients
mongoimport --host mongodb:27017 \
    --username admin \
    --password password123 \
    --authenticationDatabase admin \
    --db bigdata \
    --collection customers \
    --file /sample-data/customers.json \
    --jsonArray \
    --drop

echo "Données clients importées"

# Vérifier l'import
mongo mongodb://admin:password123@mongodb:27017/bigdata?authSource=admin --eval "
    print('=== Vérification des données ===');
    print('Nombre de ventes: ' + db.sales.count());
    print('Nombre de clients: ' + db.customers.count());
    print('Exemple de vente:');
    printjson(db.sales.findOne());
    print('Exemple de client:');
    printjson(db.customers.findOne());
"

echo "MongoDB peuplé avec succès!"