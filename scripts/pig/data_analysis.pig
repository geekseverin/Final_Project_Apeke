-- Script d'analyse exploratoire avec Apache Pig (CORRIGÉ)
-- Projet Big Data - Traitement Distribué 2024-2025

-- 1. CHARGEMENT DES DONNÉES DEPUIS HDFS (après transfert depuis MongoDB)
-- Chargement des données de ventes depuis HDFS
sales_raw = LOAD 'hdfs://hadoop-master:9000/data/sales.json' 
    USING JsonLoader('id:chararray, product:chararray, quantity:int, price:double, date:chararray, customer_id:chararray');

-- Chargement des données clients depuis HDFS
customers_raw = LOAD 'hdfs://hadoop-master:9000/data/customers.json' 
    USING JsonLoader('id:chararray, name:chararray, email:chararray, city:chararray, age:int');

-- 2. NETTOYAGE DES DONNÉES
-- Filtrer les ventes valides (prix > 0 et quantité > 0)
sales_clean = FILTER sales_raw BY price IS NOT NULL AND quantity IS NOT NULL AND price > 0 AND quantity > 0;

-- Filtrer les clients valides (âge réaliste)
customers_clean = FILTER customers_raw BY age IS NOT NULL AND age > 0 AND age < 120;

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

-- Trier les villes par revenus
city_revenue_sorted = ORDER city_revenue BY city_revenue DESC;

-- Analyse 4: Top clients par revenus
customer_revenue = GROUP sales_with_total BY customer_id;
top_customers = FOREACH customer_revenue GENERATE 
    group AS customer_id,
    COUNT(sales_with_total) AS purchase_count,
    SUM(sales_with_total.total_amount) AS customer_total;

top_customers_sorted = ORDER top_customers BY customer_total DESC;
-- CORRECTION: Syntaxe correcte pour LIMIT
top_10_customers = LIMIT top_customers_sorted 10;

-- 5. SAUVEGARDE DES RÉSULTATS DANS HDFS
STORE product_summary_sorted INTO 'hdfs://hadoop-master:9000/pig-output/product-analysis' USING PigStorage(',');
STORE city_stats INTO 'hdfs://hadoop-master:9000/pig-output/city-analysis' USING PigStorage(',');
STORE city_revenue_sorted INTO 'hdfs://hadoop-master:9000/pig-output/city-revenue' USING PigStorage(',');
STORE top_10_customers INTO 'hdfs://hadoop-master:9000/pig-output/top-customers' USING PigStorage(',');

-- 6. AFFICHAGE DES RÉSULTATS POUR DÉMONSTRATION
-- Limiter l'affichage pour éviter les erreurs
product_top5 = LIMIT product_summary_sorted 5;
customers_top5 = LIMIT top_10_customers 5;

DUMP product_top5;
DUMP city_stats;
DUMP customers_top5;