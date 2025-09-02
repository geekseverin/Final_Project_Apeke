#!/usr/bin/env python3
"""
Script Spark pour lire des données depuis MongoDB
Projet Big Data - Traitement Distribué 2024-2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Créer une session Spark avec configuration MongoDB"""
    return SparkSession.builder \
        .appName("MongoDB-Hadoop-Reader") \
        .config("spark.mongodb.input.uri", "mongodb://admin:password123@mongodb:27017/bigdata.sales") \
        .config("spark.mongodb.output.uri", "mongodb://admin:password123@mongodb:27017/bigdata.results") \
        .config("spark.jars", "/opt/hadoop/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar,/opt/hadoop/share/hadoop/common/lib/mongodb-driver-3.12.11.jar") \
        .getOrCreate()

def read_mongodb_data(spark):
    """Lire les données depuis MongoDB"""
    print("=== Lecture des données MongoDB ===")
    
    # Lire les données de ventes
    sales_df = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://admin:password123@mongodb:27017/bigdata.sales") \
        .load()
    
    print(f"Nombre de ventes chargées: {sales_df.count()}")
    sales_df.show(10)
    
    # Lire les données clients
    customers_df = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://admin:password123@mongodb:27017/bigdata.customers") \
        .load()
    
    print(f"Nombre de clients chargés: {customers_df.count()}")
    customers_df.show(10)
    
    return sales_df, customers_df

def analyze_data(sales_df, customers_df):
    """Effectuer des analyses sur les données"""
    print("=== Analyse des données avec Spark ===")
    
    # 1. Calcul du montant total par vente
    sales_with_total = sales_df.withColumn("total_amount", 
                                          col("quantity") * col("price"))
    
    # 2. Analyse des ventes par produit
    product_analysis = sales_with_total.groupBy("product") \
        .agg(
            count("*").alias("total_sales"),
            sum("quantity").alias("total_quantity"),
            avg("price").alias("avg_price"),
            sum("total_amount").alias("total_revenue")
        ) \
        .orderBy(desc("total_revenue"))
    
    print("Top 5 produits par revenus:")
    product_analysis.show(5)
    
    # 3. Jointure clients-ventes
    sales_customers = sales_with_total.join(customers_df, 
                                           sales_with_total.customer_id == customers_df.id)
    
    # 4. Analyse par ville
    city_analysis = sales_customers.groupBy("city") \
        .agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("city_revenue"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy(desc("city_revenue"))
    
    print("Revenus par ville:")
    city_analysis.show()
    
    # 5. Top clients
    customer_analysis = sales_with_total.groupBy("customer_id") \
        .agg(
            count("*").alias("purchase_count"),
            sum("total_amount").alias("customer_total")
        ) \
        .orderBy(desc("customer_total"))
    
    print("Top 5 clients:")
    customer_analysis.show(5)
    
    return {
        'product_analysis': product_analysis,
        'city_analysis': city_analysis,
        'customer_analysis': customer_analysis
    }

def save_results_to_hdfs(results, spark):
    """Sauvegarder les résultats dans HDFS"""
    print("=== Sauvegarde des résultats dans HDFS ===")
    
    # Sauvegarder l'analyse des produits
    results['product_analysis'].write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/spark-output/product-analysis")
    
    # Sauvegarder l'analyse des villes
    results['city_analysis'].write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/spark-output/city-analysis")
    
    # Sauvegarder l'analyse des clients
    results['customer_analysis'].write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("hdfs://hadoop-master:9000/spark-output/customer-analysis")
    
    print("Résultats sauvegardés dans HDFS avec succès!")

def save_results_to_mongodb(results, spark):
    """Sauvegarder les résultats dans MongoDB"""
    print("=== Sauvegarde des résultats dans MongoDB ===")
    
    # Sauvegarder l'analyse des produits dans MongoDB
    results['product_analysis'].write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://admin:password123@mongodb:27017/bigdata.product_analysis") \
        .mode("overwrite") \
        .save()
    
    print("Résultats sauvegardés dans MongoDB avec succès!")

def main():
    """Fonction principale"""
    print("Démarrage de l'analyse MongoDB avec Spark...")
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Lire les données
        sales_df, customers_df = read_mongodb_data(spark)
        
        # Analyser les données
        results = analyze_data(sales_df, customers_df)
        
        # Sauvegarder les résultats
        save_results_to_hdfs(results, spark)
        save_results_to_mongodb(results, spark)
        
        print("=== Analyse terminée avec succès! ===")
        
    except Exception as e:
        print(f"Erreur lors de l'analyse: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()