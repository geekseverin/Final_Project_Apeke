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

# CORRECTION: Créer les répertoires nécessaires
RUN mkdir -p /sample-data
RUN mkdir -p /scripts/setup
RUN mkdir -p /docker-entrypoint-initdb.d

# CORRECTION: Copier les données d'exemple AVANT le script d'init
COPY sample-data/ /sample-data/

# Vérifier que les données sont bien copiées
RUN ls -la /sample-data/

# Copier le script d'initialisation MongoDB
COPY init-mongo.js /docker-entrypoint-initdb.d/

# CORRECTION: Copier et rendre exécutable le script de population
COPY populate_mongodb.sh /scripts/setup/populate_mongodb.sh
RUN chmod +x /scripts/setup/populate_mongodb.sh

# CORRECTION: Créer un script d'init qui combine l'initialisation et la population
RUN cat > /docker-entrypoint-initdb.d/00-setup-and-populate.js << 'EOF'
// Script d'initialisation complet MongoDB
print("=== Initialisation de la base de données BigData ===");

// Créer la base de données
db = db.getSiblingDB('bigdata');

// Créer un utilisateur pour l'application
db.createUser({
    user: "bigdata_user",
    pwd: "bigdata_pass",
    roles: [
        { role: "readWrite", db: "bigdata" }
    ]
});

// Créer les collections avec des indices
db.createCollection("sales");
db.createCollection("customers");
db.createCollection("product_analysis");
db.createCollection("city_analysis");

// Créer des indices pour optimiser les performances
db.sales.createIndex({ "customer_id": 1 });
db.sales.createIndex({ "product": 1 });
db.sales.createIndex({ "date": 1 });
db.customers.createIndex({ "city": 1 });
db.customers.createIndex({ "email": 1 }, { unique: true });

print("Base de données BigData initialisée!");

// Charger et insérer les données d'exemple
print("=== Chargement des données d'exemple ===");

// Fonction pour charger un fichier JSON
function loadJSONFile(filename) {
    try {
        var content = cat(filename);
        return JSON.parse(content);
    } catch (e) {
        print("Erreur lors du chargement de " + filename + ": " + e);
        return null;
    }
}

// Charger les données de ventes
var salesData = loadJSONFile('/sample-data/sales.json');
if (salesData && salesData.length > 0) {
    db.sales.insertMany(salesData);
    print("✅ " + salesData.length + " ventes insérées");
} else {
    print("❌ Impossible de charger les données de ventes");
    // Données de fallback
    db.sales.insertMany([
        {"id": "s001", "product": "Laptop Dell XPS", "quantity": 1, "price": 1299.99, "date": "2024-01-15", "customer_id": "c001"},
        {"id": "s002", "product": "iPhone 15", "quantity": 2, "price": 999.99, "date": "2024-01-16", "customer_id": "c002"},
        {"id": "s003", "product": "Samsung Galaxy S24", "quantity": 1, "price": 899.99, "date": "2024-01-17", "customer_id": "c003"}
    ]);
    print("✅ Données de ventes de fallback insérées");
}

// Charger les données des clients
var customersData = loadJSONFile('/sample-data/customers.json');
if (customersData && customersData.length > 0) {
    db.customers.insertMany(customersData);
    print("✅ " + customersData.length + " clients insérés");
} else {
    print("❌ Impossible de charger les données des clients");
    // Données de fallback
    db.customers.insertMany([
        {"id": "c001", "name": "Alice Martin", "email": "alice.martin@email.com", "city": "Paris", "age": 28},
        {"id": "c002", "name": "Bob Dupont", "email": "bob.dupont@email.com", "city": "Lyon", "age": 35},
        {"id": "c003", "name": "Claire Moreau", "email": "claire.moreau@email.com", "city": "Marseille", "age": 42}
    ]);
    print("✅ Données de clients de fallback insérées");
}

// Vérification finale
print("=== Vérification finale ===");
print("Collections créées: " + db.getCollectionNames());
print("Nombre de ventes: " + db.sales.count());
print("Nombre de clients: " + db.customers.count());

if (db.sales.count() > 0 && db.customers.count() > 0) {
    print("🎉 Initialisation MongoDB terminée avec succès!");
} else {
    print("❌ Problème lors de l'initialisation");
}
EOF

# Exposer le port MongoDB
EXPOSE 27017

# CORRECTION: Script de démarrage personnalisé
RUN cat > /custom-entrypoint.sh << 'EOF'
#!/bin/bash
set -e

echo "🍃 Démarrage MongoDB avec initialisation personnalisée..."

# Démarrer MongoDB en arrière-plan
docker-entrypoint.sh mongod --bind_ip_all &
MONGOD_PID=$!

# Attendre que MongoDB soit prêt
echo "⏳ Attente de MongoDB..."
until mongosh --host localhost:27017 --eval "db.runCommand('ping')" --quiet >/dev/null 2>&1; do
    sleep 2
done

echo "✅ MongoDB est prêt!"

# Exécuter le script de population si nécessaire
if [ -f "/scripts/setup/populate_mongodb.sh" ]; then
    echo "🔄 Exécution du script de population..."
    /scripts/setup/populate_mongodb.sh || echo "⚠️ Script de population échoué (peut-être déjà fait)"
fi

# Attendre le processus MongoDB
wait $MONGOD_PID
EOF

RUN chmod +x /custom-entrypoint.sh

# CORRECTION: Utiliser notre script d'entrée personnalisé
# CMD ["/custom-entrypoint.sh"]

# Pour la compatibilité, garder le CMD par défaut mais permettre l'init
CMD ["mongod", "--bind_ip_all"]