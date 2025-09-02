#!/usr/bin/env python3
"""
Script Spark pour lire des données depuis HDFS et les analyser
Projet Big Data - Traitement Distribué 2024-2025
VERSION CORRIGÉE
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# CORRECTION : Import conditionnel de pymongo
try:
    import pymongo
    PYMONGO_AVAILABLE = True
    print("pymongo disponible - sauvegarde MongoDB activée")
except ImportError:
    PYMONGO_AVAILABLE = False
    print("pymongo non disponible - sauvegarde MongoDB désactivée")

def create_spark_session():
    """Créer une session Spark optimisée"""
    return SparkSession.builder \
        .appName("BigData-HDFS-Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

def read_hdfs_data(spark):
    """Lire les données depuis HDFS avec gestion d'erreurs améliorée"""
    print("=== 📊 Lecture des données HDFS ===")
    
    try:
        # Vérifier la connectivité HDFS
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        
        # Lire les données de ventes depuis HDFS
        print("📋 Lecture des données de ventes...")
        sales_path = "hdfs://hadoop-master:9000/data/sales.json"
        sales_df = spark.read.option("multiline", "true").json(sales_path)
        
        if sales_df.count() == 0:
            print("❌ Aucune donnée de vente trouvée")
            return None, None
            
        print(f"✅ Ventes chargées: {sales_df.count()} enregistrements")
        print("🔍 Structure des ventes:")
        sales_df.printSchema()
        sales_df.show(3, truncate=False)
        
        # Lire les données clients depuis HDFS
        print("\n👥 Lecture des données clients...")
        customers_path = "hdfs://hadoop-master:9000/data/customers.json"
        customers_df = spark.read.option("multiline", "true").json(customers_path)
        
        if customers_df.count() == 0:
            print("❌ Aucune donnée client trouvée")
            return None, None
            
        print(f"✅ Clients chargés: {customers_df.count()} enregistrements")
        print("🔍 Structure des clients:")
        customers_df.printSchema()
        customers_df.show(3, truncate=False)
        
        return sales_df, customers_df
        
    except Exception as e:
        print(f"❌ Erreur lecture HDFS: {str(e)}")
        print("🔧 Vérifiez que HDFS est démarré et que les données existent")
        return None, None

def validate_and_clean_data(sales_df, customers_df):
    """Nettoyer et valider les données"""
    print("\n=== 🧹 Nettoyage et validation des données ===")
    
    # Nettoyer les données de ventes
    sales_clean = sales_df.filter(
        (col("price").isNotNull()) & 
        (col("quantity").isNotNull()) & 
        (col("price") > 0) & 
        (col("quantity") > 0) &
        (col("product").isNotNull()) &
        (col("customer_id").isNotNull())
    )
    
    # Nettoyer les données clients
    customers_clean = customers_df.filter(
        (col("id").isNotNull()) &
        (col("city").isNotNull()) &
        (col("name").isNotNull())
    )
    
    print(f"📊 Ventes valides: {sales_clean.count()}/{sales_df.count()}")
    print(f"👥 Clients valides: {customers_clean.count()}/{customers_df.count()}")
    
    return sales_clean, customers_clean

def analyze_data(sales_df, customers_df):
    """Effectuer des analyses complètes sur les données"""
    print("\n=== 📈 Analyse des données avec Spark ===")
    
    if sales_df is None or customers_df is None:
        print("❌ Pas de données à analyser")
        return {}
    
    # Nettoyer les données
    sales_clean, customers_clean = validate_and_clean_data(sales_df, customers_df)
    
    # 1. Calcul du montant total par vente
    print("\n💰 Calcul des montants totaux...")
    sales_with_total = sales_clean.withColumn("total_amount", 
                                             col("quantity").cast("double") * col("price").cast("double"))
    
    # 2. Analyse des ventes par produit
    print("🛍️ Analyse des produits...")
    product_analysis = sales_with_total.groupBy("product") \
        .agg(
            count("*").alias("total_sales"),
            sum("quantity").alias("total_quantity"),
            avg("price").alias("avg_price"),
            sum("total_amount").alias("total_revenue")
        ) \
        .orderBy(desc("total_revenue"))
    
    print("🏆 Top 5 produits par revenus:")
    product_analysis.show(5, truncate=False)
    
    # 3. Jointure clients-ventes pour analyse géographique
    print("\n🔗 Jointure clients-ventes...")
    sales_customers = sales_with_total.join(
        customers_clean, 
        sales_with_total.customer_id == customers_clean.id,
        "inner"
    )
    
    join_count = sales_customers.count()
    print(f"✅ Jointure réussie: {join_count} enregistrements")
    
    if join_count > 0:
        sales_customers.show(3, truncate=False)
    
    # 4. Analyse par ville
    print("\n🌆 Analyse par ville...")
    city_analysis = sales_customers.groupBy("city") \
        .agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("city_revenue"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy(desc("city_revenue"))
    
    print("🏙️ Revenus par ville:")
    city_analysis.show(truncate=False)
    
    # 5. Top clients
    print("\n👑 Analyse des meilleurs clients...")
    customer_analysis = sales_with_total.groupBy("customer_id") \
        .agg(
            count("*").alias("purchase_count"),
            sum("total_amount").alias("customer_total")
        ) \
        .orderBy(desc("customer_total"))
    
    print("🥇 Top 5 clients:")
    customer_analysis.show(5, truncate=False)
    
    # 6. Statistiques générales
    print("\n📊 Statistiques générales:")
    total_revenue = sales_with_total.agg(sum("total_amount")).collect()[0][0]
    total_sales = sales_with_total.count()
    unique_products = sales_with_total.select("product").distinct().count()
    unique_customers = sales_with_total.select("customer_id").distinct().count()
    
    print(f"💵 Chiffre d'affaires total: {total_revenue:.2f}€")
    print(f"📦 Nombre total de ventes: {total_sales}")
    print(f"🛍️ Produits uniques: {unique_products}")
    print(f"👥 Clients uniques: {unique_customers}")
    
    return {
        'product_analysis': product_analysis,
        'city_analysis': city_analysis,
        'customer_analysis': customer_analysis,
        'sales_with_total': sales_with_total
    }

def save_results_to_hdfs(results, spark):
    """Sauvegarder les résultats dans HDFS avec gestion d'erreurs"""
    print("\n=== 💾 Sauvegarde des résultats dans HDFS ===")
    
    try:
        base_path = "hdfs://hadoop-master:9000/spark-output"
        
        # Sauvegarder l'analyse des produits
        print("📝 Sauvegarde analyse produits...")
        results['product_analysis'].coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/product-analysis")
        
        # Sauvegarder l'analyse des villes
        print("🌍 Sauvegarde analyse villes...")
        results['city_analysis'].coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/city-analysis")
        
        # Sauvegarder l'analyse des clients
        print("👤 Sauvegarde analyse clients...")
        results['customer_analysis'].coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"{base_path}/customer-analysis")
        
        print("✅ Résultats sauvegardés dans HDFS avec succès!")
        
        # Vérifier les fichiers créés
        print("\n📁 Fichiers créés dans HDFS:")
        import subprocess
        subprocess.run(["hdfs", "dfs", "-ls", "-R", "/spark-output"], check=False)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde HDFS: {str(e)}")
        return False

def save_results_to_mongodb(results, spark):
    """Sauvegarder les résultats dans MongoDB si pymongo est disponible"""
    if not PYMONGO_AVAILABLE:
        print("⚠️ pymongo non disponible - sauvegarde MongoDB ignorée")
        return False
        
    print("\n=== 🍃 Sauvegarde des résultats dans MongoDB ===")
    
    try:
        # Connexion MongoDB avec timeout
        client = pymongo.MongoClient(
            "mongodb://admin:password123@mongodb:27017/?authSource=admin",
            serverSelectionTimeoutMS=5000
        )
        
        # Test de connexion
        client.admin.command('ping')
        db = client.bigdata
        
        # Convertir et sauvegarder l'analyse des produits
        print("📊 Sauvegarde analyse produits...")
        product_data = results['product_analysis'].collect()
        
        if product_data:
            product_docs = []
            for row in product_data:
                product_docs.append({
                    'product': row['product'],
                    'total_sales': int(row['total_sales']),
                    'total_quantity': int(row['total_quantity']) if row['total_quantity'] else 0,
                    'avg_price': float(row['avg_price']) if row['avg_price'] else 0.0,
                    'total_revenue': float(row['total_revenue']) if row['total_revenue'] else 0.0,
                    'analysis_date': '2024-09-02'
                })
            
            # Supprimer et insérer
            db.product_analysis.drop()
            db.product_analysis.insert_many(product_docs)
            print(f"✅ Sauvegardé {len(product_docs)} analyses de produits dans MongoDB")
        
        # Sauvegarder analyse des villes
        print("🌍 Sauvegarde analyse villes...")
        city_data = results['city_analysis'].collect()
        
        if city_data:
            city_docs = []
            for row in city_data:
                city_docs.append({
                    'city': row['city'],
                    'total_transactions': int(row['total_transactions']),
                    'city_revenue': float(row['city_revenue']) if row['city_revenue'] else 0.0,
                    'unique_customers': int(row['unique_customers']) if row['unique_customers'] else 0,
                    'analysis_date': '2024-09-02'
                })
            
            db.city_analysis.drop()
            db.city_analysis.insert_many(city_docs)
            print(f"✅ Sauvegardé {len(city_docs)} analyses de villes dans MongoDB")
        
        client.close()
        print("✅ Résultats sauvegardés dans MongoDB avec succès!")
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde MongoDB: {str(e)}")
        return False

def main():
    """Fonction principale avec gestion d'erreurs complète"""
    print("🚀 Démarrage de l'analyse Big Data avec Spark...")
    print("=" * 60)
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print(f"✅ Session Spark créée: {spark.version}")
        print(f"🎯 Application: {spark.sparkContext.appName}")
        
        # Lire les données depuis HDFS
        sales_df, customers_df = read_hdfs_data(spark)
        
        if sales_df is not None and customers_df is not None:
            # Analyser les données
            results = analyze_data(sales_df, customers_df)
            
            if results:
                # Sauvegarder les résultats
                hdfs_success = save_results_to_hdfs(results, spark)
                mongo_success = save_results_to_mongodb(results, spark)
                
                print("\n" + "=" * 60)
                print("📊 RÉSUMÉ DE L'ANALYSE")
                print("=" * 60)
                print(f"💾 Sauvegarde HDFS: {'✅ Réussie' if hdfs_success else '❌ Échec'}")
                print(f"🍃 Sauvegarde MongoDB: {'✅ Réussie' if mongo_success else '❌ Échec'}")
                print("✅ Analyse terminée avec succès!")
            else:
                print("❌ Aucun résultat d'analyse généré")
        else:
            print("❌ Impossible de charger les données")
            sys.exit(1)
        
    except Exception as e:
        print(f"❌ Erreur critique lors de l'analyse: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        print("\n🔚 Arrêt de la session Spark...")
        spark.stop()
        print("✅ Session fermée")

if __name__ == "__main__":
    main()