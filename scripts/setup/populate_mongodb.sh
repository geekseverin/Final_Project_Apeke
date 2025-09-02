#!/bin/bash

# Script de population MongoDB avec des données d'exemple
# Projet Big Data - Traitement Distribué 2024-2025
# VERSION CORRIGÉE

set -e

echo "🍃 Population de MongoDB avec des données d'exemple..."

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Fonction pour attendre que MongoDB soit complètement prêt
wait_for_mongodb() {
    log_info "Attente de la disponibilité de MongoDB..."
    local max_attempts=60
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if mongosh --host localhost:27017 \
            --username admin \
            --password password123 \
            --authenticationDatabase admin \
            --eval "db.runCommand('ping')" \
            --quiet >/dev/null 2>&1; then
            log_success "MongoDB est prêt!"
            return 0
        fi
        
        if [ $((attempt % 10)) -eq 0 ]; then
            log_info "Tentative $attempt/$max_attempts..."
        fi
        
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_error "MongoDB n'est pas accessible après $max_attempts tentatives"
    return 1
}

# Fonction pour vérifier l'existence des fichiers
check_data_files() {
    log_info "Vérification des fichiers de données..."
    
    if [ ! -f "/sample-data/sales.json" ]; then
        log_error "Fichier /sample-data/sales.json introuvable"
        ls -la /sample-data/ || echo "Répertoire /sample-data/ introuvable"
        return 1
    fi
    
    if [ ! -f "/sample-data/customers.json" ]; then
        log_error "Fichier /sample-data/customers.json introuvable"
        ls -la /sample-data/ || echo "Répertoire /sample-data/ introuvable"
        return 1
    fi
    
    log_info "Contenu du répertoire /sample-data/:"
    ls -la /sample-data/
    
    log_info "Aperçu du fichier sales.json:"
    head -5 /sample-data/sales.json
    
    log_info "Aperçu du fichier customers.json:"
    head -5 /sample-data/customers.json
    
    log_success "Fichiers de données vérifiés"
}

# Fonction pour importer les données avec vérification
import_collection() {
    local collection=$1
    local file=$2
    
    log_info "Import de la collection $collection depuis $file..."
    
    # Vérifier que le fichier existe et n'est pas vide
    if [ ! -s "$file" ]; then
        log_error "Le fichier $file est vide ou n'existe pas"
        return 1
    fi
    
    # Import avec gestion d'erreurs détaillée
    if mongoimport --host localhost:27017 \
        --username admin \
        --password password123 \
        --authenticationDatabase admin \
        --db bigdata \
        --collection $collection \
        --file $file \
        --jsonArray \
        --drop \
        --verbose; then
        
        log_success "Collection $collection importée avec succès"
        
        # Vérifier le nombre de documents importés
        local count=$(mongosh --host localhost:27017 \
            --username admin \
            --password password123 \
            --authenticationDatabase admin \
            --db bigdata \
            --eval "db.$collection.countDocuments({})" \
            --quiet 2>/dev/null | tail -1)
            
        log_success "$count documents importés dans $collection"
        return 0
    else
        log_error "Échec de l'import de la collection $collection"
        return 1
    fi
}

# Fonction pour vérifier les données importées
verify_import() {
    log_info "Vérification des données importées..."
    
    # Script de vérification MongoDB
    mongosh --host localhost:27017 \
        --username admin \
        --password password123 \
        --authenticationDatabase admin \
        --db bigdata \
        --eval "
            print('=== Vérification des données importées ===');
            print('');
            
            // Vérifier les collections
            var collections = db.getCollectionNames();
            print('Collections disponibles: ' + collections.join(', '));
            print('');
            
            // Vérifier les ventes
            var salesCount = db.sales.countDocuments({});
            print('Nombre de ventes: ' + salesCount);
            if (salesCount > 0) {
                print('Exemple de vente:');
                printjson(db.sales.findOne());
            } else {
                print('❌ Aucune vente trouvée!');
            }
            print('');
            
            // Vérifier les clients
            var customersCount = db.customers.countDocuments({});
            print('Nombre de clients: ' + customersCount);
            if (customersCount > 0) {
                print('Exemple de client:');
                printjson(db.customers.findOne());
            } else {
                print('❌ Aucun client trouvé!');
            }
            print('');
            
            // Vérifier la cohérence des données
            print('=== Vérification de cohérence ===');
            var salesWithCustomers = db.sales.aggregate([
                {\$lookup: {
                    from: 'customers',
                    localField: 'customer_id',
                    foreignField: 'id',
                    as: 'customer'
                }},
                {\$match: {customer: {\$size: 0}}}
            ]).toArray().length;
            
            if (salesWithCustomers > 0) {
                print('⚠️ ' + salesWithCustomers + ' ventes sans client correspondant');
            } else {
                print('✅ Toutes les ventes ont un client correspondant');
            }
        " --quiet
}

# Fonction pour créer des données de test si les fichiers sont absents
create_sample_data() {
    log_warning "Création de données d'exemple de secours..."
    
    # Créer le répertoire s'il n'existe pas
    mkdir -p /sample-data
    
    # Créer un fichier sales.json de base
    cat > /sample-data/sales.json << 'EOF'
[
  {"id": "s001", "product": "Laptop Dell XPS", "quantity": 1, "price": 1299.99, "date": "2024-01-15", "customer_id": "c001"},
  {"id": "s002", "product": "iPhone 15", "quantity": 2, "price": 999.99, "date": "2024-01-16", "customer_id": "c002"},
  {"id": "s003", "product": "Samsung Galaxy S24", "quantity": 1, "price": 899.99, "date": "2024-01-17", "customer_id": "c003"},
  {"id": "s004", "product": "MacBook Pro", "quantity": 1, "price": 2299.99, "date": "2024-01-18", "customer_id": "c001"},
  {"id": "s005", "product": "iPad Air", "quantity": 3, "price": 649.99, "date": "2024-01-19", "customer_id": "c002"}
]
EOF

    # Créer un fichier customers.json de base
    cat > /sample-data/customers.json << 'EOF'
[
  {"id": "c001", "name": "Alice Martin", "email": "alice.martin@email.com", "city": "Paris", "age": 28},
  {"id": "c002", "name": "Bob Dupont", "email": "bob.dupont@email.com", "city": "Lyon", "age": 35},
  {"id": "c003", "name": "Claire Moreau", "email": "claire.moreau@email.com", "city": "Marseille", "age": 42},
  {"id": "c004", "name": "David Bernard", "email": "david.bernard@email.com", "city": "Paris", "age": 31},
  {"id": "c005", "name": "Emma Leroy", "email": "emma.leroy@email.com", "city": "Toulouse", "age": 26}
]
EOF

    log_success "Données d'exemple de secours créées"
}

# Fonction principale
main() {
    echo "🚀 Démarrage de l'initialisation MongoDB..."
    
    # Attendre que MongoDB soit prêt
    if ! wait_for_mongodb; then
        log_error "Impossible de se connecter à MongoDB"
        exit 1
    fi
    
    # Vérifier les fichiers de données
    if ! check_data_files; then
        log_warning "Fichiers de données manquants, création de données de secours..."
        create_sample_data
        
        # Re-vérifier après création
        if ! check_data_files; then
            log_error "Impossible de créer ou trouver les fichiers de données"
            exit 1
        fi
    fi
    
    # Importer les données
    local import_success=true
    
    if ! import_collection "sales" "/sample-data/sales.json"; then
        log_error "Échec de l'import des ventes"
        import_success=false
    fi
    
    if ! import_collection "customers" "/sample-data/customers.json"; then
        log_error "Échec de l'import des clients"
        import_success=false
    fi
    
    if [ "$import_success" = true ]; then
        log_success "Toutes les données ont été importées avec succès!"
        
        # Vérification finale
        verify_import
        
        echo ""
        echo "🎉 MongoDB peuplé avec succès!"
        echo "   - Base de données: bigdata"
        echo "   - Collections: sales, customers"
        echo "   - Accès: mongodb://admin:password123@mongodb:27017"
        
    else
        log_error "Certaines données n'ont pas pu être importées"
        exit 1
    fi
}

# Fonction de diagnostic en cas d'échec
diagnostic() {
    echo ""
    log_info "=== DIAGNOSTIC MONGODB ==="
    
    # Test de connexion basique
    log_info "Test de connexion MongoDB..."
    if mongosh --host localhost:27017 --eval "db.runCommand('ping')" --quiet >/dev/null 2>&1; then
        log_success "Connexion MongoDB basique OK"
    else
        log_error "Connexion MongoDB basique échoue"
    fi
    
    # Test de connexion avec authentification
    log_info "Test de connexion avec authentification..."
    if mongosh --host localhost:27017 \
        --username admin \
        --password password123 \
        --authenticationDatabase admin \
        --eval "db.runCommand('ping')" --quiet >/dev/null 2>&1; then
        log_success "Connexion avec authentification OK"
    else
        log_error "Connexion avec authentification échoue"
    fi
    
    # Vérifier les variables d'environnement
    log_info "Variables d'environnement MongoDB:"
    env | grep MONGO || echo "Aucune variable MONGO trouvée"
    
    # Vérifier les processus MongoDB
    log_info "Processus MongoDB:"
    ps aux | grep mongod || echo "Aucun processus mongod trouvé"
}

# Gestion des erreurs
trap 'log_error "Script interrompu"; diagnostic; exit 1' ERR

# Point d'entrée
if [ "$1" = "--diagnostic" ]; then
    diagnostic
    exit 0
elif [ "$1" = "--help" ]; then
    echo "Script de population MongoDB"
    echo ""
    echo "Usage: $0 [option]"
    echo ""
    echo "Options:"
    echo "  --diagnostic  Effectuer un diagnostic MongoDB"
    echo "  --help        Afficher cette aide"
    echo ""
    exit 0
else
    main "$@"
fi