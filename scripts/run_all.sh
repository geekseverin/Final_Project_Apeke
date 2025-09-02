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
docker exec mongodb /scripts/setup/populate_mongodb.sh 2>/dev/null || log_warning "MongoDB déjà initialisé"

# 4. Créer les répertoires HDFS nécessaires
log_info "Création des répertoires HDFS..."
docker exec hadoop-master hdfs dfs -mkdir -p /data || true
docker exec hadoop-master hdfs dfs -mkdir -p /pig-output || true
docker exec hadoop-master hdfs dfs -mkdir -p /spark-output || true
docker exec hadoop-master hdfs dfs -chmod 777 /data || true
docker exec hadoop-master hdfs dfs -chmod 777 /pig-output || true
docker exec hadoop-master hdfs dfs -chmod 777 /spark-output || true
log_success "Répertoires HDFS créés"

# 5. Créer des données CSV simples directement dans HDFS (solution alternative)
log_info "Création des données CSV pour Pig..."

# Créer sales.csv directement
docker exec hadoop-master bash -c 'cat > /tmp/sales.csv << EOF
id,product,quantity,price,date,customer_id
s001,Laptop Dell XPS,1,1299.99,2024-01-15,c001
s002,iPhone 15,2,999.99,2024-01-16,c002
s003,Samsung Galaxy S24,1,899.99,2024-01-17,c003
s004,MacBook Pro,1,2299.99,2024-01-18,c004
s005,iPad Air,3,649.99,2024-01-19,c005
s006,AirPods Pro,2,279.99,2024-01-20,c001
s007,Laptop Dell XPS,1,1299.99,2024-01-21,c006
s008,Samsung Galaxy S24,2,899.99,2024-01-22,c007
s009,iPhone 15,1,999.99,2024-01-23,c008
s010,MacBook Pro,1,2299.99,2024-01-24,c009
s011,AirPods Pro,4,279.99,2024-01-25,c010
s012,iPad Air,1,649.99,2024-01-26,c002
s013,Laptop Dell XPS,2,1299.99,2024-01-27,c003
s014,iPhone 15,1,999.99,2024-01-28,c011
s015,Samsung Galaxy S24,3,899.99,2024-01-29,c012
EOF'

# Créer customers.csv directement
docker exec hadoop-master bash -c 'cat > /tmp/customers.csv << EOF
id,name,email,city,age
c001,Alice Martin,alice.martin@email.com,Paris,28
c002,Bob Dupont,bob.dupont@email.com,Lyon,35
c003,Claire Moreau,claire.moreau@email.com,Marseille,42
c004,David Bernard,david.bernard@email.com,Paris,31
c005,Emma Leroy,emma.leroy@email.com,Toulouse,26
c006,François Petit,francois.petit@email.com,Nice,39
c007,Gabrielle Roux,gabrielle.roux@email.com,Lyon,33
c008,Henri Blanc,henri.blanc@email.com,Strasbourg,45
c009,Isabelle Girard,isabelle.girard@email.com,Paris,29
c010,Julien Morel,julien.morel@email.com,Bordeaux,37
c011,Karine Fournier,karine.fournier@email.com,Marseille,41
c012,Laurent Simon,laurent.simon@email.com,Toulouse,34
EOF'

# Copier vers HDFS
docker exec hadoop-master hdfs dfs -put -f /tmp/sales.csv /data/
docker exec hadoop-master hdfs dfs -put -f /tmp/customers.csv /data/

# Nettoyer les fichiers temporaires
docker exec hadoop-master rm -f /tmp/sales.csv /tmp/customers.csv

log_success "Données CSV créées et copiées vers HDFS"

# Vérifier les données dans HDFS
log_info "Vérification des données dans HDFS..."
docker exec hadoop-master hdfs dfs -ls /data/
docker exec hadoop-master hdfs dfs -head /data/sales.csv

# 6. Exécuter l'analyse Pig
log_info "Exécution de l'analyse Apache Pig..."
docker exec hadoop-master pig -f /scripts/pig/data_analysis.pig 2>/dev/null
if [ $? -eq 0 ]; then
    log_success "Analyse Pig terminée avec succès"
else
    log_warning "Erreur lors de l'analyse Pig - vérifiez les logs"
    # Afficher les logs pour debug
    docker exec hadoop-master tail -20 /home/hadoop/pig_*.log 2>/dev/null || true
fi

# 7. Exécuter l'analyse Spark (optionnel)
log_info "Tentative d'exécution de l'analyse Apache Spark..."
if docker exec hadoop-master test -f /scripts/spark/mongodb_reader.py; then
    docker exec hadoop-master spark-submit \
        --jars /opt/hadoop/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar,/opt/hadoop/share/hadoop/common/lib/mongodb-driver-3.12.11.jar \
        --conf "spark.mongodb.input.uri=mongodb://admin:password123@mongodb:27017/bigdata.sales" \
        --conf "spark.mongodb.output.uri=mongodb://admin:password123@mongodb:27017/bigdata.results" \
        /scripts/spark/mongodb_reader.py 2>/dev/null
    if [ $? -eq 0 ]; then
        log_success "Analyse Spark terminée avec succès"
    else
        log_warning "Erreur lors de l'analyse Spark"
    fi
else
    log_warning "Script Spark non trouvé - ignoré"
fi

# 8. Vérifier l'état du cluster et les résultats
log_info "Vérification des résultats d'analyse..."
echo ""
echo "=== RÉSULTATS DES ANALYSES ==="

# Vérifier les résultats Pig
if docker exec hadoop-master hdfs dfs -test -d /pig-output/product-analysis 2>/dev/null; then
    echo "✅ Analyse Pig des produits: Terminée"
    echo "Aperçu des résultats:"
    docker exec hadoop-master hdfs dfs -head /pig-output/product-analysis/part-r-00000 2>/dev/null || true
else
    echo "❌ Analyse Pig des produits: Non trouvée"
fi

if docker exec hadoop-master hdfs dfs -test -d /pig-output/city-revenue 2>/dev/null; then
    echo "✅ Analyse Pig des villes: Terminée"
    echo "Aperçu des résultats:"
    docker exec hadoop-master hdfs dfs -head /pig-output/city-revenue/part-r-00000 2>/dev/null || true
else
    echo "❌ Analyse Pig des villes: Non trouvée"
fi

echo ""
echo "=== ÉTAT DU CLUSTER ==="
echo "🌐 Hadoop NameNode: http://localhost:9870"
echo "⚙️  Yarn ResourceManager: http://localhost:8088"
echo "⚡ Spark Master: http://localhost:8080"
echo "📊 Dashboard Web: http://localhost:5000"
echo "🍃 MongoDB: mongodb://localhost:27017"
echo ""

# Test de connectivité pour les services web
echo "=== TEST DE CONNECTIVITÉ ==="
test_url() {
    local url=$1
    local name=$2
    if curl -s -f "$url" > /dev/null 2>&1; then
        echo "✅ $name: Accessible"
    else
        echo "❌ $name: Non accessible - Vérifiez que le port est exposé"
    fi
}

test_url "http://localhost:9870" "Hadoop NameNode"
test_url "http://localhost:8088" "Yarn ResourceManager"  
test_url "http://localhost:8080" "Spark Master"
test_url "http://localhost:5000" "Dashboard Web"

# Afficher un résumé des services
echo ""
echo "=== STATUT DES CONTENEURS ==="
docker-compose ps

echo ""
log_success "🎉 Cluster Big Data configuré!"
echo ""
echo "Si les services web ne sont pas accessibles, vérifiez:"
echo "  1. Les ports sont bien exposés dans docker-compose.yml"
echo "  2. Les services sont démarrés: docker-compose logs [service-name]"
echo "  3. Redémarrez si nécessaire: docker-compose restart"
echo ""
echo "Pour arrêter le cluster: docker-compose down -v"