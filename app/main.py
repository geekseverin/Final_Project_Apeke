def read_hdfs_analysis_results(analysis_type):
    """Lire les résultats d'analyse depuis HDFS (VERSION CORRIGÉE)"""
    try:
        # Essayer plusieurs chemins possibles
        possible_paths = [
            f"hdfs://hadoop-master:9000/pig-output/{analysis_type}/part-r-00000",
            f"hdfs://hadoop-master:9000/pig-output/{analysis_type}/part-m-00000",
            f"hdfs://hadoop-master:9000/pig-output/{analysis_type}/*"
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
                        if line.strip():
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