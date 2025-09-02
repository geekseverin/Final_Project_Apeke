from flask import Flask, render_template, jsonify
import pymongo
import subprocess
import json
import os
from datetime import datetime

app = Flask(__name__)

# Configuration MongoDB
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:password123@mongodb:27017/bigdata?authSource=admin')

def get_mongodb_connection():
    """Obtenir une connexion MongoDB"""
    try:
        client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        # Test de la connexion
        client.admin.command('ping')
        return client
    except Exception as e:
        print(f"Erreur connexion MongoDB: {e}")
        return None

def read_hdfs_analysis_results(analysis_type):
    """Lire les résultats d'analyse depuis HDFS (VERSION CORRIGÉE)"""
    try:
        # Essayer plusieurs chemins possibles
        possible_paths = [
            f"/pig-output/{analysis_type}/part-r-00000",
            f"/pig-output/{analysis_type}/part-m-00000",
            f"/spark-output/{analysis_type}/part-00000-*.csv"
        ]
        
        for hdfs_path in possible_paths:
            try:
                # Utiliser hdfs dfs -cat pour lire le fichier
                result = subprocess.run([
                    'docker', 'exec', 'hadoop-master', 
                    'hdfs', 'dfs', '-cat', hdfs_path
                ], capture_output=True, text=True, timeout=10)
                
                if result.returncode == 0 and result.stdout.strip():
                    print(f"Lecture réussie depuis {hdfs_path}")
                    # Parser les données CSV
                    lines = result.stdout.strip().split('\n')
                    data = []
                    for line in lines:
                        if line.strip() and not line.startswith('product,') and not line.startswith('city,'):
                            parts = line.split(',')
                            try:
                                if analysis_type == 'product-analysis' and len(parts) >= 5:
                                    data.append({
                                        'product': parts[0].strip(),
                                        'total_sales': int(float(parts[1])),
                                        'total_quantity': int(float(parts[2])),
                                        'avg_price': float(parts[3]),
                                        'total_revenue': float(parts[4])
                                    })
                                elif analysis_type == 'city-revenue' and len(parts) >= 3:
                                    data.append({
                                        'city': parts[0].strip(),
                                        'total_transactions': int(float(parts[1])),
                                        'city_revenue': float(parts[2])
                                    })
                            except (ValueError, IndexError) as e:
                                print(f"Erreur parsing ligne '{line}': {e}")
                                continue
                    
                    if data:
                        return data
                        
            except subprocess.TimeoutExpired:
                print(f"Timeout lors de la lecture de {hdfs_path}")
                continue
            except Exception as e:
                print(f"Erreur lecture {hdfs_path}: {e}")
                continue
        
        print(f"Aucun fichier trouvé pour {analysis_type}")
        return []
            
    except Exception as e:
        print(f"Erreur générale lecture analyse HDFS {analysis_type}: {e}")
        return []

@app.route('/')
def dashboard():
    """Page principale du dashboard"""
    return render_template('dashboard.html')

@app.route('/api/cluster-status')
def cluster_status():
    """API pour obtenir l'état du cluster"""
    try:
        # Vérifier Hadoop
        hadoop_result = subprocess.run([
            'docker', 'exec', 'hadoop-master', 'hdfs', 'dfsadmin', '-report'
        ], capture_output=True, text=True, timeout=10)
        hadoop_status = "Online ✅" if hadoop_result.returncode == 0 else "Offline ❌"
        
        # Vérifier Spark
        spark_result = subprocess.run([
            'docker', 'exec', 'hadoop-master', 'curl', '-s', 'http://localhost:8080'
        ], capture_output=True, text=True, timeout=5)
        spark_status = "Online ✅" if spark_result.returncode == 0 else "Offline ❌"
        
        # Vérifier MongoDB
        client = get_mongodb_connection()
        mongodb_status = "Online ✅" if client else "Offline ❌"
        if client:
            client.close()
        
        return jsonify({
            'hadoop_status': hadoop_status,
            'spark_status': spark_status,
            'mongodb_status': mongodb_status,
            'last_update': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Erreur statut cluster: {e}")
        return jsonify({
            'hadoop_status': 'Unknown',
            'spark_status': 'Unknown',
            'mongodb_status': 'Unknown',
            'last_update': datetime.now().isoformat()
        })

@app.route('/api/sales-summary')
def sales_summary():
    """API pour obtenir le résumé des ventes"""
    try:
        client = get_mongodb_connection()
        if not client:
            return jsonify({'error': 'MongoDB connection failed'}), 500
            
        db = client.bigdata
        
        # Calculer les métriques depuis MongoDB
        total_sales = db.sales.count_documents({})
        total_customers = db.customers.count_documents({})
        
        # Calculer le chiffre d'affaires total
        pipeline = [
            {"$addFields": {"total": {"$multiply": ["$quantity", "$price"]}}},
            {"$group": {"_id": None, "total_revenue": {"$sum": "$total"}}}
        ]
        revenue_result = list(db.sales.aggregate(pipeline))
        total_revenue = revenue_result[0]['total_revenue'] if revenue_result else 0
        
        # Trouver le produit le plus vendu
        top_product_pipeline = [
            {"$group": {
                "_id": "$product", 
                "total_quantity": {"$sum": "$quantity"},
                "total_revenue": {"$sum": {"$multiply": ["$quantity", "$price"]}}
            }},
            {"$sort": {"total_revenue": -1}},
            {"$limit": 1}
        ]
        top_product_result = list(db.sales.aggregate(top_product_pipeline))
        top_product = top_product_result[0]['_id'] if top_product_result else "N/A"
        
        client.close()
        
        return jsonify({
            'total_sales': total_sales,
            'total_customers': total_customers,
            'total_revenue': round(total_revenue, 2),
            'top_product': {'name': top_product}
        })
        
    except Exception as e:
        print(f"Erreur résumé ventes: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/product-analysis')
def product_analysis():
    """API pour l'analyse des produits"""
    try:
        # Essayer de lire depuis MongoDB d'abord (résultats Spark)
        client = get_mongodb_connection()
        if client:
            db = client.bigdata
            if db.product_analysis.count_documents({}) > 0:
                results = list(db.product_analysis.find({}, {'_id': 0}))
                client.close()
                return jsonify(results)
            client.close()
        
        # Sinon, essayer de lire depuis HDFS
        hdfs_results = read_hdfs_analysis_results('product-analysis')
        if hdfs_results:
            return jsonify(hdfs_results)
        
        # En dernier recours, calculer depuis MongoDB directement
        client = get_mongodb_connection()
        if client:
            db = client.bigdata
            pipeline = [
                {"$addFields": {"total": {"$multiply": ["$quantity", "$price"]}}},
                {"$group": {
                    "_id": "$product",
                    "total_sales": {"$sum": 1},
                    "total_quantity": {"$sum": "$quantity"},
                    "avg_price": {"$avg": "$price"},
                    "total_revenue": {"$sum": "$total"}
                }},
                {"$project": {
                    "product": "$_id",
                    "total_sales": 1,
                    "total_quantity": 1,
                    "avg_price": {"$round": ["$avg_price", 2]},
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "_id": 0
                }},
                {"$sort": {"total_revenue": -1}}
            ]
            results = list(db.sales.aggregate(pipeline))
            client.close()
            return jsonify(results)
        
        return jsonify([])
        
    except Exception as e:
        print(f"Erreur analyse produits: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/city-analysis')
def city_analysis():
    """API pour l'analyse par ville"""
    try:
        client = get_mongodb_connection()
        if not client:
            return jsonify([])
            
        db = client.bigdata
        
        # Agrégation pour joindre sales et customers
        pipeline = [
            {
                "$lookup": {
                    "from": "customers",
                    "localField": "customer_id",
                    "foreignField": "id",
                    "as": "customer"
                }
            },
            {"$unwind": "$customer"},
            {"$addFields": {"total": {"$multiply": ["$quantity", "$price"]}}},
            {
                "$group": {
                    "_id": "$customer.city",
                    "total_transactions": {"$sum": 1},
                    "city_revenue": {"$sum": "$total"}
                }
            },
            {
                "$project": {
                    "city": "$_id",
                    "total_transactions": 1,
                    "city_revenue": {"$round": ["$city_revenue", 2]},
                    "_id": 0
                }
            },
            {"$sort": {"city_revenue": -1}}
        ]
        
        results = list(db.sales.aggregate(pipeline))
        client.close()
        return jsonify(results)
        
    except Exception as e:
        print(f"Erreur analyse villes: {e}")
        client = get_mongodb_connection()
        if client:
            client.close()
        return jsonify([])

@app.route('/api/analysis-status')
def analysis_status():
    """API pour vérifier l'état des analyses"""
    try:
        # Vérifier les analyses Pig dans HDFS
        pig_result = subprocess.run([
            'docker', 'exec', 'hadoop-master', 'hdfs', 'dfs', '-ls', '/pig-output'
        ], capture_output=True, text=True, timeout=10)
        pig_available = pig_result.returncode == 0
        
        # Vérifier les analyses Spark dans HDFS
        spark_result = subprocess.run([
            'docker', 'exec', 'hadoop-master', 'hdfs', 'dfs', '-ls', '/spark-output'
        ], capture_output=True, text=True, timeout=10)
        spark_available = spark_result.returncode == 0
        
        # Vérifier les données brutes MongoDB
        client = get_mongodb_connection()
        raw_data_available = False
        if client:
            db = client.bigdata
            raw_data_available = (db.sales.count_documents({}) > 0 and 
                                db.customers.count_documents({}) > 0)
            client.close()
        
        return jsonify({
            'pig_analysis_available': pig_available,
            'spark_analysis_available': spark_available,
            'raw_data_available': raw_data_available
        })
        
    except Exception as e:
        print(f"Erreur statut analyses: {e}")
        return jsonify({
            'pig_analysis_available': False,
            'spark_analysis_available': False,
            'raw_data_available': False
        })

if __name__ == '__main__':
    print("🚀 Démarrage de l'application Web Big Data...")
    print("📊 Dashboard disponible sur: http://localhost:5000")
    
    # Attendre que MongoDB soit prêt
    import time
    for i in range(30):
        client = get_mongodb_connection()
        if client:
            client.close()
            break
        print(f"Attente de MongoDB... ({i+1}/30)")
        time.sleep(2)
    
    app.run(host='0.0.0.0', port=5000, debug=True)