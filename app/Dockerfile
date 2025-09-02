FROM python:3.9-slim

RUN pip install pymongo

# Installer les dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Créer le répertoire de travail
WORKDIR /app

# Copier les requirements et installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l'application
COPY . .

# Exposer le port
EXPOSE 5000

# Variables d'environnement
ENV FLASK_APP=main.py
ENV FLASK_ENV=development

# Commande de démarrage
CMD ["python", "main.py"]