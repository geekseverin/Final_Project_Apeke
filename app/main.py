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
    """Obtenir une connexion MongoDB avec gestion d'erreurs améliorée"""
    try:
        client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000)
        # Test de la connexion
        client.admin.command('ping')
        return client
    except Exception as e:
        print(f"Erreur connexion MongoDB: {e}")
        return None

def execute_docker_command(command, timeout=15):
    """Exécuter une commande Docker avec gestion d'erreurs"""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=isinstance(command, str)
        )
        return result
    except subprocess.TimeoutExpired:
        print(f"Timeout lors de l'exécution: {command}")
        return None
    except Exception as e:
        print(f"Erreur exécution commande: {e}")
        return None

def read_hdfs_analysis_results(analysis_type):
    """CORRIGÉ: Lire les résultats d'analyse depuis HDFS"""
    try:
        print(f"Tentative de lecture {analysis_type} depuis HDFS...")
        
        # Chemins possibles dans HDFS
        possible_paths = [
            f"/spark-output/{analysis_type}/*.csv",
            f"/spark-output/{analysis_type}/part-00000*",
            f"/pig-output/{analysis_type}/part-r-00000",
            f"/pig-output/{analysis_type}/part-m-00000"
        ]
        
        for hdfs_path in possible_paths:
            try:
                print(f"Essai du chemin: {hdfs_path}")
                
                # Lister les fichiers disponibles
                list_result = execute_docker_command([
                    'docker', 'exec', 'hadoop-master',
                    'hdfs', 'dfs', '-ls', f'/{analysis_type}*'
                ])
                
                if list_result and list_result.returncode == 0:
                    print(f"Fichiers trouvés: {list_result.stdout}")
                
                # Essayer de lire le contenu
                result = execute_docker_command([
                    'docker', 'exec', 'hadoop-master',
                    'hdfs', 'dfs', '-cat', hdfs_path
                ])
                
                if result and result.returncode == 0 and result.stdout.strip():
                    print(f"Lecture réussie depuis {hdfs_path}")
                    return parse_csv_data(result.stdout, analysis_type)
                    
            except Exception as e:
                print(f"Erreur lecture {hdfs_path}: {e}")
                continue
        
        print(f"Aucun fichier HDFS trouvé pour {analysis_type}")
        return []
            
    except Exception as e:
        print(f"Erreur générale lecture HDFS {analysis_type}: {e}")
        return []

def parse_csv_data(csv_content, analysis_type):
    """Parser les données CSV depuis HDFS"""
    try:
        lines = [line.strip() for line in csv_content.strip().split('\n') if line.strip()]
        data = []
        
        for line in lines:
            # Ignorer les en-têtes
            if 'product,' in line.lower() or 'city,' in line.lower():
                continue
                
            parts = [part.strip() for part in line.split(',')]
            
            try:
                if analysis_type == 'product-analysis' and len(parts) >= 4:
                    data.append({
                        'product': parts[0],
                        'total_sales': int(float(parts[1])) if parts[1] else 0,
                        'total_quantity': int(float(parts[2])) if parts[2] else 0,
                        'avg_price': float(parts[3]) if parts[3] else 0.0,
                        'total_revenue': float(parts[4]) if len(parts) > 4 and parts[4] else 0.0
                    })
                elif analysis_type == 'city-analysis' and len(parts) >= 2:
                    data.append({
                        'city': parts[0],
                        'total_transactions': int(float(parts[1])) if parts[1] else 0,
                        'city_revenue': float(parts[2]) if len(parts) > 2 and parts[2] else 0.0
                    })
            except (ValueError, IndexError) as e:
                print(f"Erreur parsing ligne '{line}': {e}")
                continue
        
        return data
        
    except Exception as e:
        print(f"Erreur parsing CSV: {e}")
        return []

@app.route('/')
def dashboard():
    """Page principale du dashboard"""
    return render_template('dashboard.html')

@app.route('/api/cluster-status')
def cluster_status():
    """CORRIGÉ: API pour obtenir l'état du cluster"""
    try:
        # Vérifier Hadoop NameNode
        hadoop_result = execute_docker_command([
            'docker', 'exec', 'hadoop-master', 'curl', '-s', '-f',
            'http://localhost:9870/dfshealth.html'
        ], timeout=5)
        hadoop_status = "Online ✅" if (hadoop_result and hadoop_result.returncode == 0) else "Offline ❌"
        
        # Vérifier Yarn ResourceManager
        yarn_result = execute_docker_command([
            'docker', 'exec', 'hadoop-master', 'curl', '-s', '-f',
            'http://localhost:8088'
        ], timeout=5)
        yarn_status = "Online ✅" if (yarn_result and yarn_result.returncode == 0) else "Offline ❌"
        
        # Vérifier Spark Master
        spark_result = execute_docker_command([
            'docker', 'exec', 'hadoop-master', 'curl', '-s', '-f',
            'http://localhost:8080'
        ], timeout=5)
        spark_status = "Online ✅" if (spark_result and spark_result.returncode == 0) else "Offline ❌"
        
        # Vérifier MongoDB
        client = get_mongodb_connection()
        mongodb_status = "Online ✅" if client else "Offline ❌"
        if client:
            client.close()
        
        return jsonify({
            'hadoop_status': hadoop_status,
            'yarn_status': yarn_status,
            'spark_status': spark_status,
            'mongodb_status': mongodb_status,
            'last_update': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Erreur statut cluster: {e}")
        return jsonify({
            'hadoop_status': 'Unknown ❓',
            'yarn_status': 'Unknown ❓',
            'spark_status': 'Unknown ❓',
            'mongodb_status': 'Unknown ❓',
            'last_update': datetime.now().isoformat()
        })

@app.route('/api/sales-summary')
def sales_summary():
    """CORRIGÉ: API pour obtenir le résumé des ventes"""
    try:
        client = get_mongodb_connection()
        if not client:
            return jsonify({
                'error': 'MongoDB connection failed',
                'total_sales': 0,
                'total_customers': 0,
                'total_revenue': 0,
                'top_product': {'name': 'N/A'}
            }), 500
            
        db = client.bigdata
        
        # Calculer les métriques depuis MongoDB avec gestion d'erreurs
        try:
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
            
        except Exception as mongo_error:
            print(f"Erreur requête MongoDB: {mongo_error}")
            total_sales = total_customers = total_revenue = 0
            top_product = "N/A"
        
        client.close()
        
        return jsonify({
            'total_sales': total_sales,
            'total_customers': total_customers,
            'total_revenue': round(total_revenue, 2),
            'top_product': {'name': top_product}
        })
        
    except Exception as e:
        print(f"Erreur résumé ventes: {e}")
        return jsonify({
            'error': str(e),
            'total_sales': 0,
            'total_customers': 0,
            'total_revenue': 0,
            'top_product': {'name': 'N/A'}
        }), 500

@app.route('/api/product-analysis')
def product_analysis():
    """CORRIGÉ: API pour l'analyse des produits avec sources multiples"""
    try:
        # 1. Essayer MongoDB d'abord (résultats Spark sauvegardés)
        client = get_mongodb_connection()
        if client:
            db = client.bigdata
            mongo_results = list(db.product_analysis.find({}, {'_id': 0}))
            if mongo_results:
                print("Données produits trouvées dans MongoDB")
                client.close()
                return jsonify(mongo_results)
            client.close()
        
        # 2. Essayer HDFS (résultats Spark ou Pig)
        print("Tentative lecture HDFS pour product-analysis...")
        hdfs_results = read_hdfs_analysis_results('product-analysis')
        if hdfs_results:
            print(f"Données produits trouvées dans HDFS: {len(hdfs_results)} éléments")
            return jsonify(hdfs_results)
        
        # 3. Calculer depuis MongoDB directement (fallback)
        print("Calcul direct depuis MongoDB...")
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
            print(f"Calcul direct réussi: {len(results)} produits")
            return jsonify(results)
        
        # 4. Aucune donnée disponible
        print("Aucune donnée produit disponible")
        return jsonify([])
        
    except Exception as e:
        print(f"Erreur analyse produits: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/city-analysis')
def city_analysis():
    """CORRIGÉ: API pour l'analyse par ville avec sources multiples"""
    try:
        # 1. Essayer MongoDB d'abord (résultats Spark sauvegardés)
        client = get_mongodb_connection()
        if client:
            db = client.bigdata
            mongo_results = list(db.city_analysis.find({}, {'_id': 0}))
            if mongo_results:
                print("Données villes trouvées dans MongoDB")
                client.close()
                return jsonify(mongo_results)
            client.close()
        
        # 2. Essayer HDFS
        print("Tentative lecture HDFS pour city-analysis...")
        hdfs_results = read_hdfs_analysis_results('city-analysis')
        if hdfs_results:
            print(f"Données villes trouvées dans HDFS: {len(hdfs_results)} éléments")
            return jsonify(hdfs_results)
        
        # 3. Calculer directement depuis MongoDB (fallback)
        print("Calcul direct des données par ville...")
        client = get_mongodb_connection()
        if client:
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
            print(f"Calcul direct réussi: {len(results)} villes")
            return jsonify(results)
        
        return jsonify([])
        
    except Exception as e:
        print(f"Erreur analyse villes: {e}")
        return jsonify([])

@app.route('/api/analysis-status')
def analysis_status():
    """CORRIGÉ: API pour vérifier l'état des analyses"""
    try:
        # Vérifier les analyses Pig dans HDFS
        pig_result = execute_docker_command([
            'docker', 'exec', 'hadoop-master', 'hdfs', 'dfs', '-ls', '/pig-output'
        ])
        pig_available = pig_result and pig_result.returncode == 0
        
        # Vérifier les analyses Spark dans HDFS
        spark_result = execute_docker_command([
            'docker', 'exec', 'hadoop-master', 'hdfs', 'dfs', '-ls', '/spark-output'
        ])
        spark_available = spark_result and spark_result.returncode == 0
        
        # Vérifier les données dans MongoDB
        client = get_mongodb_connection()
        raw_data_available = False
        processed_data_available = False
        
        if client:
            db = client.bigdata
            try:
                raw_data_available = (
                    db.sales.count_documents({}) > 0 and 
                    db.customers.count_documents({}) > 0
                )
                processed_data_available = (
                    db.product_analysis.count_documents({}) > 0 or
                    db.city_analysis.count_documents({}) > 0
                )
            except Exception as mongo_error:
                print(f"Erreur vérification MongoDB: {mongo_error}")
            finally:
                client.close()
        
        # Vérifier les données dans HDFS
        hdfs_data_result = execute_docker_command([
            'docker', 'exec', 'hadoop-master', 'hdfs', 'dfs', '-ls', '/data'
        ])
        hdfs_data_available = hdfs_data_result and hdfs_data_result.returncode == 0
        
        return jsonify({
            'pig_analysis_available': pig_available,
            'spark_analysis_available': spark_available,
            'raw_data_available': raw_data_available,
            'processed_data_available': processed_data_available,
            'hdfs_data_available': hdfs_data_available,
            'analysis_timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Erreur statut analyses: {e}")
        return jsonify({
            'pig_analysis_available': False,
            'spark_analysis_available': False,
            'raw_data_available': False,
            'processed_data_available': False,
            'hdfs_data_available': False,
            'error': str(e)
        })

@app.route('/api/system-info')
def system_info():
    """NOUVEAU: API pour informations système détaillées"""
    try:
        # Informations HDFS
        hdfs_info = {}
        hdfs_report = execute_docker_command([
            'docker', 'exec', 'hadoop-master', 'hdfs', 'dfsadmin', '-report'
        ])
        if hdfs_report and hdfs_report.returncode == 0:
            hdfs_info['status'] = 'Online ✅'
            hdfs_info['details'] = 'HDFS opérationnel'
        else:
            hdfs_info['status'] = 'Offline ❌'
            hdfs_info['details'] = 'HDFS indisponible'
        
        # Informations MongoDB
        mongo_info = {}
        client = get_mongodb_connection()
        if client:
            try:
                server_info = client.server_info()
                mongo_info['status'] = 'Online ✅'
                mongo_info['version'] = server_info.get('version', 'Unknown')
                mongo_info['details'] = f"MongoDB {mongo_info['version']}"
            except:
                mongo_info['status'] = 'Error ❌'
                mongo_info['details'] = 'Erreur connexion MongoDB'
            finally:
                client.close()
        else:
            mongo_info['status'] = 'Offline ❌'
            mongo_info['details'] = 'MongoDB indisponible'
        
        # Informations Spark
        spark_info = {}
        spark_status = execute_docker_command([
            'docker', 'exec', 'hadoop-master', 'curl', '-s', '-f', 'http://localhost:8080'
        ])
        if spark_status and spark_status.returncode == 0:
            spark_info['status'] = 'Online ✅'
            spark_info['details'] = 'Spark Master actif'
        else:
            spark_info['status'] = 'Offline ❌'
            spark_info['details'] = 'Spark Master indisponible'
        
        return jsonify({
            'hdfs': hdfs_info,
            'mongodb': mongo_info,
            'spark': spark_info,
            'last_check': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Erreur informations système: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("🚀 Démarrage de l'application Web Big Data...")
    print("📊 Dashboard disponible sur: http://localhost:5000")
    
    # Attendre que MongoDB soit prêt avec plus de patience
    import time
    max_retries = 60  # 2 minutes
    for i in range(max_retries):
        client = get_mongodb_connection()
        if client:
            print("✅ MongoDB est prêt!")
            client.close()
            break
        print(f"⏳ Attente de MongoDB... ({i+1}/{max_retries})")
        time.sleep(2)
    else:
        print("⚠️ Démarrage sans MongoDB (sera réessayé lors des requêtes)")
    
    # Démarrer l'application
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)