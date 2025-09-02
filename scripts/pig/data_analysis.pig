-- Script d'analyse exploratoire avec Apache Pig (VERSION CORRIGÉE SANS JSON)
-- Projet Big Data - Traitement Distribué 2024-2025

-- 1. CONVERSION DES DONNÉES JSON EN CSV DANS HDFS PUIS CHARGEMENT
-- Cette approche évite les problèmes de JsonLoader

-- Script pour vérifier l'existence des données
fs -ls /data/;

-- Chargement des données de ventes depuis CSV converti
-- Format: id,product,quantity,price,date,customer_id
sales_raw = LOAD '/data/sales.csv' USING PigStorage(',') 
    AS (id:chararray, product:chararray, quantity:int, price:double, date:chararray, customer_id:chararray);

-- Chargement des données clients depuis CSV converti  
-- Format: id,name,email,city,age
customers_raw = LOAD '/data/customers.csv' USING PigStorage(',')
    AS (id:chararray, name:chararray, email:chararray, city:chararray, age:int);

-- 2. NETTOYAGE ET VALIDATION DES DONNÉES
-- Afficher quelques échantillons pour debug
sales_sample = LIMIT sales_raw 3;
DUMP sales_sample;

customers_sample = LIMIT customers_raw 3;
DUMP customers_sample;

-- Filtrer les ventes valides (prix > 0 et quantité > 0)
sales_clean = FILTER sales_raw BY 
    price IS NOT NULL AND 
    quantity IS NOT NULL AND 
    price > 0 AND 
    quantity > 0 AND
    product IS NOT NULL AND
    customer_id IS NOT NULL;

-- Filtrer les clients valides
customers_clean = FILTER customers_raw BY 
    age IS NOT NULL AND 
    age > 0 AND 
    age < 120 AND
    city IS NOT NULL AND
    id IS NOT NULL;

-- Afficher le nombre d'enregistrements après nettoyage
sales_count_group = GROUP sales_clean ALL;
sales_total = FOREACH sales_count_group GENERATE COUNT(sales_clean) as total;
DUMP sales_total;

customers_count_group = GROUP customers_clean ALL;
customers_total = FOREACH customers_count_group GENERATE COUNT(customers_clean) as total;
DUMP customers_total;

-- 3. TRANSFORMATIONS ET CALCULS
-- Calculer le montant total par vente
sales_with_total = FOREACH sales_clean GENERATE 
    id,
    product,
    quantity,
    price,
    (double)(quantity * price) AS total_amount,
    customer_id,
    date;

-- 4. ANALYSES PRINCIPALES

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

-- Analyse 2: Jointure clients-ventes pour analyse par ville
sales_customers = JOIN sales_with_total BY customer_id, customers_clean BY id;

-- Vérifier la jointure
join_sample = LIMIT sales_customers 2;
DUMP join_sample;

-- Ventes par ville
sales_by_city = GROUP sales_customers BY customers_clean::city;
city_revenue = FOREACH sales_by_city GENERATE 
    group AS city,
    COUNT(sales_customers) AS total_transactions,
    SUM(sales_customers.sales_with_total::total_amount) AS city_revenue;

-- Trier les villes par revenus
city_revenue_sorted = ORDER city_revenue BY city_revenue DESC;

-- Analyse 3: Top clients par revenus
customer_revenue = GROUP sales_with_total BY customer_id;
top_customers = FOREACH customer_revenue GENERATE 
    group AS customer_id,
    COUNT(sales_with_total) AS purchase_count,
    SUM(sales_with_total.total_amount) AS customer_total;

top_customers_sorted = ORDER top_customers BY customer_total DESC;
top_10_customers = LIMIT top_customers_sorted 10;

-- 5. SAUVEGARDE DES RÉSULTATS DANS HDFS
-- Supprimer les anciens résultats s'ils existent
fs -rmr /pig-output/product-analysis;
fs -rmr /pig-output/city-revenue;
fs -rmr /pig-output/top-customers;

-- Sauvegarder avec format CSV
STORE product_summary_sorted INTO '/pig-output/product-analysis' USING PigStorage(',');
STORE city_revenue_sorted INTO '/pig-output/city-revenue' USING PigStorage(',');
STORE top_10_customers INTO '/pig-output/top-customers' USING PigStorage(',');

-- 6. AFFICHAGE DES RÉSULTATS POUR DÉMONSTRATION
product_top5 = LIMIT product_summary_sorted 5;
customers_top5 = LIMIT top_10_customers 5;
cities_all = LIMIT city_revenue_sorted 10;

-- Afficher les résultats
DESCRIBE product_top5;
DUMP product_top5;

DESCRIBE cities_all;  
DUMP cities_all;

DESCRIBE customers_top5;
DUMP customers_top5;

-- Vérifier que les fichiers ont été créés
fs -ls /pig-output/;
fs -ls /pig-output/product-analysis/;
fs -ls /pig-output/city-revenue/;
fs -ls /pig-output/top-customers/;