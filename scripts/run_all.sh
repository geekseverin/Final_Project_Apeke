#!/bin/bash

# Script principal pour initialiser et démarrer le cluster Big Data
# Projet Big Data - Traitement Distribué 2024-2025
# VERSION CORRIGÉE

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

# Fonction pour attendre qu'un service soit prêt (CORRIGÉE)
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=60  # 2 minutes
    local attempt=1
    
    log_info "Attente du démarrage de $service_name sur $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec hadoop-master bash -c "nc -z $host $port" 2>/dev/null; then
            log_success "$service_name est prêt!"
            return 0
        fi
        
        if [ $((attempt % 10)) -eq 0 ]; then
            echo ""
            log_info "Tentative $attempt/$max_attempts pour $service_name..."
        else
            echo -n "."
        fi
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo ""
    log_error "Timeout: $service_name n'a pas démarré dans les temps"
    return 1
}

# Fonction pour vérifier l'état des conteneurs
check_containers() {
    log_info "Vérification de l'état des conteneurs..."
    
    local containers=("hadoop-master" "hadoop-worker1" "hadoop-worker2" "hadoop-worker3" "mongodb" "web-app")
    local all_running=true
    
    for container in "${containers[@]}"; do
        if docker ps --format "table {{.Names}}" | grep -q "^${container}$"; then
            log_success "✅ $container est en cours d'exécution"
        else
            log_error "❌ $container n'est pas en cours d'exécution"
            all_running=false
        fi
    done
    
    if [ "$all_running" = false ]; then
        log_error "Certains conteneurs ne sont pas démarrés. Lancez 'docker-compose up -d' d'abord."
        exit 1
    fi
}

# Fonction pour initialiser MongoDB avec plus de robustesse
init_mongodb() {
    log_info "Initialisation des données MongoDB..."
    
    # Attendre que MongoDB soit complètement prêt
    local mongodb_ready=false
    for i in {1..30}; do
        if docker exec mongodb mongosh --host localhost:27017 --eval "db.runCommand('ping')" 2>/dev/null | grep -q "ok"; then
            mongodb_ready=true
            break
        fi
        echo -n "."
        sleep 2
    done
    
    if [ "$mongodb_ready" = false ]; then
        log_error "MongoDB n'est pas prêt après 1 minute"
        return 1
    fi
    
    echo ""
    log_success "MongoDB est prêt"
    
    # Exécuter le script de population
    if docker exec mongodb /scripts/setup/populate_mongodb.sh; then
        log_success "Données MongoDB initialisées avec succès"
        
        # Vérifier les données
        local sales_count=$(docker exec mongodb mongosh --host localhost:27017 \
            --username admin --password password123 --authenticationDatabase admin \
            --db bigdata --eval "db.sales.countDocuments({})" --quiet 2>/dev/null | tail -n1)
        local customers_count=$(docker exec mongodb mongosh --host localhost:27017 \
            --username admin --password password123 --authenticationDatabase admin \
            --db bigdata --eval "db.customers.countDocuments({})" --quiet 2>/dev/null | tail -n1)
            
        log_info "Vérification des données: $sales_count ventes, $customers_count clients"
        return 0
    else
        log_warning "Erreur lors de l'initialisation MongoDB (peut-être déjà fait)"
        return 1
    fi
}

# Fonction pour créer les répertoires HDFS
setup_hdfs_directories() {
    log_info "Configuration des répertoires HDFS..."
    
    local directories=(
        "/user/hadoop/input"
        "/user/hadoop/output" 
        "/data"
        "/pig-data"
        "/pig-output"
        "/spark-output"
        "/spark-logs"
    )
    
    for dir in "${directories[@]}"; do
        log_info "Création du répertoire $dir..."
        docker exec hadoop-master hdfs dfs -mkdir -p "$dir" 2>/dev/null || true
        docker exec hadoop-master hdfs dfs -chmod 777 "$dir" 2>/dev/null || true
    done
    
    log_success "Répertoires HDFS configurés"
}

# Fonction pour transférer les données vers HDFS (CORRIGÉE)
transfer_data_to_hdfs() {
    log_info "Transfert des données MongoDB vers HDFS..."
    
    # Vérifier que MongoDB contient des données
    local sales_count=$(docker exec mongodb mongosh --host localhost:27017 \
        --username admin --password password123 --authenticationDatabase admin \
        --db bigdata --eval "db.sales.countDocuments({})" --quiet 2>/dev/null | tail -n1)
    
    if [ "$sales_count" = "0" ] || [ -z "$sales_count" ]; then
        log_error "Aucune donnée trouvée dans MongoDB"
        return 1
    fi
    
    log_info "Export de $sales_count ventes depuis MongoDB..."
    
    # Export depuis MongoDB vers fichiers temporaires
    docker exec mongodb mongoexport --host localhost:27017 \
        --username admin --password password123 --authenticationDatabase admin \
        --db bigdata --collection sales --out /tmp/sales.json --jsonArray
    
    docker exec mongodb mongoexport --host localhost:27017 \
        --username admin --password password123 --authenticationDatabase admin \
        --db bigdata --collection customers --out /tmp/customers.json --jsonArray
    
    # Vérifier que les fichiers ont été créés
    if ! docker exec mongodb test -s /tmp/sales.json; then
        log_error "Échec de l'export des ventes"
        return 1
    fi
    
    if ! docker exec mongodb test -s /tmp/customers.json; then
        log_error "Échec de l'export des clients"
        return 1
    fi
    
    log_info "Export réussi, transfert vers HDFS..."
    
    # Transférer vers HDFS via pipe
    docker exec mongodb cat /tmp/sales.json | \
        docker exec -i hadoop-master hdfs dfs -put -f - /data/sales.json
    
    docker exec mongodb cat /tmp/customers.json | \
        docker exec -i hadoop-master hdfs dfs -put -f - /data/customers.json
    
    # Nettoyer les fichiers temporaires
    docker exec mongodb rm -f /tmp/sales.json /tmp/customers.json
    
    # Vérifier dans HDFS
    if docker exec hadoop-master hdfs dfs -test -e /data/sales.json && \
       docker exec hadoop-master hdfs dfs -test -e /data/customers.json; then
        log_success "Données transférées vers HDFS avec succès"
        
        # Afficher les détails
        docker exec hadoop-master hdfs dfs -ls /data/
        return 0
    else
        log_error "Erreur lors du transfert vers HDFS"
        return 1
    fi
}

# Fonction pour exécuter l'analyse Spark (CORRIGÉE)
run_spark_analysis() {
    log_info "Exécution de l'analyse Apache Spark..."
    
    # Vérifier que les données existent dans HDFS
    if ! docker exec hadoop-master hdfs dfs -test -e /data/sales.json; then
        log_error "Données sales.json introuvables dans HDFS"
        return 1
    fi
    
    if ! docker exec hadoop-master hdfs dfs -test -e /data/customers.json; then
        log_error "Données customers.json introuvables dans HDFS"
        return 1
    fi
    
    log_info "Données HDFS vérifiées, lancement de Spark..."
    
    # Exécuter le script Spark avec gestion d'erreurs
    if docker exec hadoop-master spark-submit \
        --master local[2] \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
        --driver-memory 1g \
        --executor-memory 1g \
        /scripts/spark/mongodb_reader.py; then
        
        log_success "Analyse Spark terminée avec succès"
        
        # Vérifier les résultats
        log_info "Vérification des résultats Spark dans HDFS..."
        docker exec hadoop-master hdfs dfs -ls /spark-output/ 2>/dev/null || true
        
        return 0
    else
        log_error "Erreur lors de l'analyse Spark"
        return 1
    fi
}

# Fonction pour exécuter l'analyse Pig (optionnelle)
run_pig_analysis() {
    log_info "Exécution de l'analyse Apache Pig..."
    
    if docker exec hadoop-master pig -x local -f /scripts/pig/data_analysis.pig; then
        log_success "Analyse Pig terminée avec succès"
        
        # Vérifier les résultats
        log_info "Vérification des résultats Pig dans HDFS..."
        docker exec hadoop-master hdfs dfs -ls /pig-output/ 2>/dev/null || true
        
        return 0
    else
        log_warning "Erreur lors de l'analyse Pig (pas critique)"
        return 1
    fi
}

# Fonction principale
main() {
    echo "=" * 80
    echo "🚀 INITIALISATION CLUSTER BIG DATA"
    echo "=" * 80
    
    # 1. Vérifier les conteneurs
    check_containers
    
    # 2. Attendre que les services critiques soient prêts
    log_info "Attente des services critiques..."
    wait_for_service hadoop-master 9870 "Hadoop NameNode" || exit 1
    wait_for_service hadoop-master 8088 "Yarn ResourceManager" || exit 1
    wait_for_service hadoop-master 7077 "Spark Master" || exit 1
    wait_for_service mongodb 27017 "MongoDB" || exit 1
    
    # 3. Initialiser MongoDB
    init_mongodb || log_warning "MongoDB peut déjà être initialisé"
    
    # 4. Configurer HDFS
    setup_hdfs_directories
    
    # 5. Transférer les données
    if transfer_data_to_hdfs; then
        log_success "Transfert de données réussi"
    else
        log_error "Échec du transfert de données"
        exit 1
    fi
    
    # 6. Exécuter les analyses
    run_spark_analysis || log_warning "Analyse Spark échouée"
    run_pig_analysis || log_warning "Analyse Pig échouée"
    
    # 7. Résumé final
    echo ""
    echo "=" * 80
    echo "📊 CLUSTER BIG DATA PRÊT"
    echo "=" * 80
    echo "🌐 Hadoop NameNode: http://localhost:9870"
    echo "⚙️  Yarn ResourceManager: http://localhost:8088"  
    echo "⚡ Spark Master: http://localhost:8080"
    echo "📊 Dashboard Web: http://localhost:5000"
    echo "🍃 MongoDB: mongodb://localhost:27017"
    echo "=" * 80
    
    # Afficher l'état des conteneurs
    echo ""
    log_info "État des conteneurs:"
    docker-compose ps
    
    echo ""
    log_success "🎉 Cluster Big Data prêt à utiliser!"
    echo ""
    echo "Commandes utiles:"
    echo "  🔍 Analyser avec Pig: docker exec hadoop-master pig -f /scripts/pig/data_analysis.pig"
    echo "  ⚡ Analyser avec Spark: docker exec hadoop-master spark-submit /scripts/spark/mongodb_reader.py"
    echo "  📁 Voir HDFS: docker exec hadoop-master hdfs dfs -ls /"
    echo "  🛑 Arrêter: docker-compose down -v"
}

# Point d'entrée
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "Script d'initialisation du cluster Big Data"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -h, --help    Afficher cette aide"
    echo "  --skip-pig    Ignorer l'analyse Pig"
    echo "  --skip-spark  Ignorer l'analyse Spark"
    echo ""
    exit 0
fi

# Exécuter le script principal
main "$@"