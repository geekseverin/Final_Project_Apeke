#!/bin/bash

# Script principal pour initialiser et démarrer le cluster Big Data
# Projet Big Data - Traitement Distribué 2024-2025

set -e

echo "🚀 Démarrage du cluster Big Data..."

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonction pour afficher des messages colorés
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Fonction pour attendre qu'un service soit prêt
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=30
    local attempt=1
    
    log_info "Attente du démarrage de $service_name sur $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec hadoop-master nc -z $host $port 2>/dev/null; then
            log_success "$service_name est prêt!"
            return 0
        fi
        
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_error "Timeout: $service_name n'a pas démarré dans les temps"
    return 1
}

# 1. Vérifier que Docker Compose est en cours d'exécution
log_info "Vérification des conteneurs..."
if ! docker-compose ps | grep -q "Up"; then
    log_error "Les conteneurs ne sont pas démarrés. Lancez 'docker-compose up -d' d'abord."
    exit 1
fi

# 2. Attendre que les services critiques soient prêts
wait_for_service hadoop-master 9870 "Hadoop NameNode"
wait_for_service hadoop-master 8088 "Yarn ResourceManager"
wait_for_service hadoop-master 7077 "Spark Master"
wait_for_service mongodb 27017 "MongoDB"

# 3. Initialiser les données MongoDB
log_info "Initialisation des données MongoDB..."
docker exec mongodb /scripts/setup/populate_mongodb.sh
if [ $? -eq 0 ]; then
    log_success "Données MongoDB initialisées"
else
    log_warning "Erreur lors de l'initialisation MongoDB (peut-être déjà fait)"
fi

# 4. Créer les répertoires HDFS nécessaires
log_info "Création des répertoires HDFS..."
docker exec hadoop-master hdfs dfs -mkdir -p /user/hadoop/input
docker exec hadoop-master hdfs dfs -mkdir -p /user/hadoop/output
docker exec hadoop-master hdfs dfs -mkdir -p /pig-data
docker exec hadoop-master hdfs dfs -mkdir -p /pig-output
docker exec hadoop-master hdfs dfs -mkdir -p /spark-output
docker exec hadoop-master hdfs dfs -chmod 777 /user/hadoop/input
docker exec hadoop-master hdfs dfs -chmod 777 /user/hadoop/output
docker exec hadoop-master hdfs dfs -chmod 777 /pig-output
docker exec hadoop-master hdfs dfs -chmod 777 /spark-output
log_success "Répertoires HDFS créés"

# 4.5. Transférer les données MongoDB vers HDFS
log_info "Transfert des données MongoDB vers HDFS..."
/transfer_mongodb_to_hdfs.sh
if [ $? -eq 0 ]; then
    log_success "Données transférées vers HDFS"
else
    log_error "Erreur lors du transfert vers HDFS"
fi

# 5. Exécuter l'analyse Pig
log_info "Exécution de l'analyse Apache Pig..."
docker exec hadoop-master pig -f /scripts/pig/data_analysis.pig
if [ $? -eq 0 ]; then
    log_success "Analyse Pig terminée avec succès"
else
    log_error "Erreur lors de l'analyse Pig"
fi

# 6. Exécuter l'analyse Spark
log_info "Exécution de l'analyse Apache Spark..."
docker exec hadoop-master spark-submit \
    --jars /opt/hadoop/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar,/opt/hadoop/share/hadoop/common/lib/mongodb-driver-3.12.11.jar \
    --conf "spark.mongodb.input.uri=mongodb://admin:password123@mongodb:27017/bigdata.sales" \
    --conf "spark.mongodb.output.uri=mongodb://admin:password123@mongodb:27017/bigdata.results" \
    /scripts/spark/mongodb_reader.py

if [ $? -eq 0 ]; then
    log_success "Analyse Spark terminée avec succès"
else
    log_error "Erreur lors de l'analyse Spark"
fi

# 7. Vérifier l'état du cluster
log_info "Vérification de l'état du cluster..."
echo ""
echo "=== ÉTAT DU CLUSTER ==="
echo "🌐 Hadoop NameNode: http://localhost:9870"
echo "⚙️  Yarn ResourceManager: http://localhost:8088"
echo "⚡ Spark Master: http://localhost:8080"
echo "📊 Dashboard Web: http://localhost:5000"
echo "🍃 MongoDB: mongodb://localhost:27017"
echo ""

# Afficher un résumé des services
docker-compose ps

echo ""
log_success "🎉 Cluster Big Data prêt à utiliser!"
echo ""
echo "Pour exécuter des analyses supplémentaires:"
echo "  - Pig: docker exec hadoop-master pig -f /scripts/pig/data_analysis.pig"
echo "  - Spark: docker exec hadoop-master spark-submit /scripts/spark/mongodb_reader.py"
echo ""
echo "Pour arrêter le cluster: docker-compose down -v"