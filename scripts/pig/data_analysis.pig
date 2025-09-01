-- Script d'analyse exploratoire avec Apache Pig
-- Projet Big Data - Traitement Distribué 2024-2025

-- Enregistrer les JAR MongoDB
REGISTER '/opt/hadoop/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar';
REGISTER '/opt/hadoop/share/hadoop/common/lib/mongodb-driver-3.12.11.jar';

-- 1. CHARGEMENT DES DONNÉES DEPUIS MONGODB
-- Chargement des données de ventes depuis MongoDB
sales_raw = LOAD 'mongodb://admin:password123@mongodb:27017/bigdata.sales' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, product:chararray, quantity:int, price:double, date:chararray, customer_id:chararray');

-- Chargement des données clients
customers_raw = LOAD 'mongodb://admin:password123@mongodb:27017/bigdata.customers' 
    USING com.mongodb.hadoop.pig.MongoLoader('id:chararray, name:chararray, email:chararray, city:chararray, age:int');

-- 2. NETTOYAGE DES DONNÉES
-- Filtrer les ventes valides (prix > 0 et quantité > 0)
sales_clean = FILTER sales_raw BY price > 0 AND quantity > 0;

-- Filtrer les clients valides (âge réaliste)
customers_clean = FILTER customers_raw BY age > 0 AND age < 120;

-- 3. TRANSFORMATIONS ET CALCULS
-- Calculer le montant total par vente
sales_with_total = FOREACH sales_clean GENERATE 
    id,
    product,
    quantity,
    price,
    (quantity * price) AS total_amount,
    customer_id,
    date;

-- 4. ANALYSES EXPLORATOIRES

-- Analyse 1: Ventes par produit
sales_by_product = GROUP sales_with_total BY product;
product_summary = FOREACH sales_by_product GENERATE 
    group AS product,
    COUNT(sales_with_total) AS total_sales,
    SUM(sales_with_total.quantity) AS total_quantity,
    AVG(sales_with_total.price) AS avg_price,
    SUM(sales_with_total.total_amount) AS total_revenue;

-- Trier par revenus décroissants
product_summary_sorted = ORDER product_summary BY total_revenue DESC;

-- Analyse 2: Statistiques clients par ville
customers_by_city = GROUP customers_clean BY city;
city_stats = FOREACH customers_by_city GENERATE 
    group AS city,
    COUNT(customers_clean) AS customer_count,
    AVG(customers_clean.age) AS avg_age;

-- Analyse 3: Jointure clients-ventes pour analyse complète
sales_customers = JOIN sales_with_total BY customer_id, customers_clean BY id;

-- Ventes par ville
sales_by_city = GROUP sales_customers BY customers_clean::city;
city_revenue = FOREACH sales_by_city GENERATE 
    group AS city,
    COUNT(sales_customers) AS total_transactions,
    SUM(sales_customers.sales_with_total::total_amount) AS city_revenue;

-- Analyse 4: Top clients par revenus
customer_revenue = GROUP sales_with_total BY customer_id;
top_customers = FOREACH customer_revenue GENERATE 
    group AS customer_id,
    COUNT(sales_with_total) AS purchase_count,
    SUM(sales_with_total.total_amount) AS customer_total;

top_customers_sorted = ORDER top_customers BY customer_total DESC;
top_10_customers = LIMIT top_customers_sorted 10;

-- 5. SAUVEGARDE DES RÉSULTATS

-- Sauvegarder dans HDFS
STORE product_summary_sorted INTO '/pig-output/product-analysis' USING PigStorage(',');
STORE city_stats INTO '/pig-output/city-analysis' USING PigStorage(',');
STORE city_revenue INTO '/pig-output/city-revenue' USING PigStorage(',');
STORE top_10_customers INTO '/pig-output/top-customers' USING PigStorage(',');

-- 6. AFFICHAGE DES RÉSULTATS POUR DÉMONSTRATION
DUMP (LIMIT product_summary_sorted 5);
DUMP city_stats;
DUMP (LIMIT top_10_customers 5);