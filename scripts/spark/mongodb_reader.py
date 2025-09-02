#!/usr/bin/env python3
"""
Script Spark pour lire des données depuis HDFS (au lieu de MongoDB directement)
Projet Big Data - Traitement Distribué 2024-2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import pymongo

def create_spark_session():
    """Créer une session Spark simple"""
    return SparkSession.builder \
        .appName("HDFS-Data-Reader") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def read_hdfs_data(spark):
    """Lire les données depuis HDFS"""
    print("=== Lecture des données HDFS ===")
    
    try:
        # Lire les données de ventes depuis HDFS
        sales_df = spark.read.json("hdfs://hadoop-master:9000/data/sales.json")
        print(f"Nombre de ventes chargées: {sales_df.count()}")
        sales_df.show(5)
        
        # Lire les données clients depuis HDFS
        customers_df = spark.read.json("hdfs://hadoop-master:9000/data/customers.json")
        print(f"Nombre de clients chargés: {customers_df.count()}")
        customers_df.show(5)
        
        return sales_df, customers_df
        
    except Exception as e:
        print(f"Erreur lecture HDFS: {e}")
        return None, None

def analyze_data(sales_df, customers_df):
    """Effectuer des analyses sur les données"""
    print("=== Analyse des données avec Spark ===")
    
    if sales_df is None or customers_df is None:
        print("Pas de données à analyser")
        return {}
    
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
    
    try:
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
        
    except Exception as e:
        print(f"Erreur sauvegarde HDFS: {e}")

def save_results_to_mongodb(results, spark):
    """Sauvegarder les résultats dans MongoDB via pymongo"""
    print("=== Sauvegarde des résultats dans MongoDB ===")
    
    try:
        # Connexion MongoDB
        client = pymongo.MongoClient("mongodb://admin:password123@mongodb:27017/?authSource=admin")
        db = client.bigdata
        
        # Convertir et sauvegarder l'analyse des produits
        product_data = results['product_analysis'].collect()
        product_docs = []
        for row in product_data:
            product_docs.append({
                'product': row['product'],
                'total_sales': int(row['total_sales']),
                'total_quantity': int(row['total_quantity']),
                'avg_price': float(row['avg_price']),
                'total_revenue': float(row['total_revenue'])
            })
        
        # Supprimer et insérer
        db.product_analysis.drop()
        if product_docs:
            db.product_analysis.insert_many(product_docs)
            print(f"Sauvegardé {len(product_docs)} analyses de produits dans MongoDB")
        
        client.close()
        print("Résultats sauvegardés dans MongoDB avec succès!")
        
    except Exception as e:
        print(f"Erreur sauvegarde MongoDB: {e}")

def main():
    """Fonction principale"""
    print("Démarrage de l'analyse HDFS avec Spark...")
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Lire les données depuis HDFS
        sales_df, customers_df = read_hdfs_data(spark)
        
        if sales_df is not None and customers_df is not None:
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