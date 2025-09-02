#!/usr/bin/env python3
"""
Application Web Flask pour visualiser les résultats d'analyse Big Data (CORRIGÉE)
Projet Big Data - Traitement Distribué 2024-2025
"""

from flask import Flask, render_template, jsonify
import pymongo
import pandas as pd
import json
from datetime import datetime
import os
import subprocess

app = Flask(__name__)

# Configuration MongoDB
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:password123@mongodb:27017/bigdata?authSource=admin')

def get_mongodb_client():
    """Obtenir une connexion MongoDB"""
    try:
        client = pymongo.MongoClient(MONGODB_URI)
        return client
    except Exception as e:
        print(f"Erreur connexion MongoDB: {e}")
        return None

def get_collection_data(collection_name):
    """Récupérer les données d'une collection MongoDB"""
    client = get_mongodb_client()
    if client is None:
        return []
    
    try:
        db = client.bigdata
        collection = db[collection_name]
        data = list(collection.find({}, {'_id': 0}))
        client.close()
        return data
    except Exception as e:
        print(f"Erreur lors de la récupération de {collection_name}: {e}")
        return []

def read_hdfs_analysis_results(analysis_type):
    """Lire les résultats d'analyse depuis HDFS"""
    try:
        hdfs_path = f"hdfs://hadoop-master:9000/pig-output/{analysis_type}"
        
        # Utiliser hdfs dfs -cat pour lire le fichier
        result = subprocess.run([
            'docker', 'exec', 'hadoop-master', 
            'hdfs', 'dfs', '-cat', f'{hdfs_path}/part-r-00000'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            # Parser les données CSV
            lines = result.stdout.strip().split('\n')
            data = []
            for line in lines:
                if line.strip():
                    parts = line.split(',')
                    if analysis_type == 'product-analysis' and len(parts) >= 5:
                        data.append({
                            'product': parts[0],
                            'total_sales': int(parts[1]),
                            'total_quantity': int(parts[2]),
                            'avg_price': float(parts[3]),
                            'total_revenue': float(parts[4])
                        })
                    elif analysis_type == 'city-revenue' and len(parts) >= 3:
                        data.append({
                            'city': parts[0],
                            'total_transactions': int(parts[1]),
                            'city_revenue': float(parts[2])
                        })
            return data
        else:
            print(f"Erreur lecture HDFS {analysis_type}: {result.stderr}")
            return []
            
    except Exception as e:
        print(f"Erreur lecture analyse HDFS {analysis_type}: {e}")
        return []

@app.route('/')
def dashboard():
    """Page principale du dashboard"""
    return render_template('dashboard.html')

@app.route('/api/sales-summary')
def sales_summary():
    """API pour récupérer le résumé des ventes"""
    try:
        sales_data = get_collection_data('sales')
        customers_data = get_collection_data('customers')
        
        # Statistiques générales
        total_sales = len(sales_data)
        total_customers = len(customers_data)
        
        # Calcul du chiffre d'affaires total
        total_revenue = sum(item['quantity'] * item['price'] for item in sales_data)
        
        # Produit le plus vendu
        product_counts = {}
        for sale in sales_data:
            product = sale['product']
            product_counts[product] = product_counts.get(product, 0) + sale['quantity']
        
        top_product = max(product_counts.items(), key=lambda x: x[1]) if product_counts else ("N/A", 0)
        
        return jsonify({
            'total_sales': total_sales,
            'total_customers': total_customers,
            'total_revenue': round(total_revenue, 2),
            'top_product': {
                'name': top_product[0],
                'quantity': top_product[1]
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/product-analysis')
def product_analysis():
    """API pour l'analyse des produits (depuis résultats Pig)"""
    try:
        # D'abord essayer de lire les résultats d'analyse Pig depuis HDFS
        pig_results = read_hdfs_analysis_results('product-analysis')
        
        if pig_results:
            print("Utilisation des résultats d'analyse Pig depuis HDFS")
            return jsonify(pig_results)
        
        # Fallback : essayer depuis MongoDB (résultats Spark)
        analysis_data = get_collection_data('product_analysis')
        if analysis_data:
            print("Utilisation des résultats d'analyse Spark depuis MongoDB")
            return jsonify(analysis_data)
        
        # Fallback final : calculer à partir des données brutes
        print("Calcul à partir des données brutes MongoDB")
        sales_data = get_collection_data('sales')
        
        product_stats = {}
        for sale in sales_data:
            product = sale['product']
            if product not in product_stats:
                product_stats[product] = {
                    'product': product,
                    'total_sales': 0,
                    'total_quantity': 0,
                    'total_revenue': 0,
                    'prices': []
                }
            
            product_stats[product]['total_sales'] += 1
            product_stats[product]['total_quantity'] += sale['quantity']
            product_stats[product]['total_revenue'] += sale['quantity'] * sale['price']
            product_stats[product]['prices'].append(sale['price'])
        
        # Calculer prix moyen et trier
        result = []
        for stats in product_stats.values():
            stats['avg_price'] = sum(stats['prices']) / len(stats['prices'])
            del stats['prices']  # Supprimer la liste des prix
            result.append(stats)
        
        result.sort(key=lambda x: x['total_revenue'], reverse=True)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/city-analysis')
def city_analysis():
    """API pour l'analyse par ville (depuis résultats Pig)"""
    try:
        # D'abord essayer de lire les résultats d'analyse Pig depuis HDFS
        pig_results = read_hdfs_analysis_results('city-revenue')
        
        if pig_results:
            print("Utilisation des résultats d'analyse Pig depuis HDFS")
            return jsonify(pig_results)
        
        # Fallback : calculer à partir des données brutes
        print("Calcul à partir des données brutes MongoDB")
        customers_data = get_collection_data('customers')
        sales_data = get_collection_data('sales')
        
        # Créer un mapping customer_id -> city
        customer_city_map = {customer['id']: customer['city'] for customer in customers_data}
        
        # Analyser les ventes par ville
        city_stats = {}
        for sale in sales_data:
            customer_id = sale['customer_id']
            city = customer_city_map.get(customer_id, 'Unknown')
            
            if city not in city_stats:
                city_stats[city] = {
                    'city': city,
                    'total_transactions': 0,
                    'city_revenue': 0,
                    'unique_customers': set()
                }
            
            city_stats[city]['total_transactions'] += 1
            city_stats[city]['city_revenue'] += sale['quantity'] * sale['price']
            city_stats[city]['unique_customers'].add(customer_id)
        
        # Convertir en liste et calculer le nombre de clients uniques
        result = []
        for stats in city_stats.values():
            stats['unique_customers'] = len(stats['unique_customers'])
            result.append(stats)
        
        result.sort(key=lambda x: x['city_revenue'], reverse=True)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analysis-status')
def analysis_status():
    """API pour vérifier l'état des analyses"""
    try:
        status = {
            'pig_analysis_available': False,
            'spark_analysis_available': False,
            'raw_data_available': False
        }
        
        # Vérifier si les résultats Pig sont disponibles dans HDFS
        pig_results = read_hdfs_analysis_results('product-analysis')
        status['pig_analysis_available'] = len(pig_results) > 0
        
        # Vérifier si les résultats Spark sont disponibles dans MongoDB
        spark_results = get_collection_data('product_analysis')
        status['spark_analysis_available'] = len(spark_results) > 0
        
        # Vérifier si les données brutes sont disponibles
        raw_sales = get_collection_data('sales')
        status['raw_data_available'] = len(raw_sales) > 0
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/cluster-status')
def cluster_status():
    """API pour vérifier le statut du cluster"""
    try:
        # Vérifier MongoDB
        client = get_mongodb_client()
        mongodb_status = "Connected" if client else "Disconnected"
        if client:
            client.close()
        
        # Informations sur le cluster (simulées pour la démo)
        return jsonify({
            'mongodb_status': mongodb_status,
            'hadoop_status': 'Running',
            'spark_status': 'Running',
            'last_update': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    """Point de santé de l'application"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)