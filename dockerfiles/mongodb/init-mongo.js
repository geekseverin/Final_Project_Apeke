// Script d'initialisation MongoDB
// Projet Big Data - Traitement Distribué 2024-2025

print("Initialisation de la base de données BigData...");

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

print("Base de données BigData initialisée avec succès!");