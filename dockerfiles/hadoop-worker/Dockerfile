FROM bigdata-project_hadoop-base:latest

USER root

# Script de démarrage pour les workers
COPY start-worker.sh /start-worker.sh
RUN chmod +x /start-worker.sh

USER hadoop

CMD ["/start-worker.sh"]