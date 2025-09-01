# Projet Big Data - Traitement Distribué 2024-2025

## Description
Projet d'ingestion et d'analyse de données distribuées utilisant Hadoop, Spark, MongoDB et Apache Pig dans un environnement Docker.

## Architecture
- **Master Node** : NameNode, ResourceManager, Spark Master
- **Secondary Master** : SecondaryNameNode
- **3 Worker Nodes** : DataNodes, NodeManagers, Spark Workers
- **MongoDB** : Base de données NoSQL pour stockage
- **Application Web** : Interface Flask pour visualisation

## Prérequis
- Docker et Docker Compose installés
- Au moins 8GB de RAM disponible
- Ports 8088, 9870, 8080, 5000, 27017 libres

## Installation et Lancement

### 1. Cloner et préparer l'environnement
```bash
git clone <your-repo>
cd bigdata-project
chmod +x scripts/*.sh scripts/setup/*.sh
```

### 2. Démarrer le cluster complet
```bash
# Lancer tous les services
docker-compose up -d

# Vérifier que tous les conteneurs sont actifs
docker-compose ps
```

### 3. Initialiser Hadoop et Spark
```bash
# Formater HDFS et démarrer les services
./scripts/run_all.sh
```

### 4. Accéder aux interfaces Web
- **Hadoop HDFS** : http://localhost:9870
- **Yarn ResourceManager** : http://localhost:8088
- **Spark Master** : http://localhost:8080
- **Application Web** : http://localhost:5000
- **MongoDB** : mongodb://localhost:27017

## Utilisation

### Analyse avec Apache Pig
```bash
# Exécuter l'analyse exploratoire
docker exec hadoop-master pig -f /scripts/pig/data_analysis.pig
```

### Lecture MongoDB avec Spark
```bash
# Exécuter le script de lecture MongoDB
docker exec hadoop-master spark-submit /scripts/spark/mongodb_reader.py
```

### Application Web
L'application Flask propose un dashboard simple pour visualiser les résultats d'analyse.

## Structure des Données
- Données d'exemple stockées dans MongoDB
- Analyses réalisées avec Pig et Spark
- Résultats accessibles via l'interface web

## Arrêt du Système
```bash
docker-compose down -v
```

## Dépannage
- Vérifier les logs : `docker-compose logs <service-name>`
- Redémarrer un service : `docker-compose restart <service-name>`
- Accès aux conteneurs : `docker exec -it <container-name> bash`