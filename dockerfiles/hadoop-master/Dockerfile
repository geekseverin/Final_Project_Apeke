FROM bigdata-project_hadoop-base:latest

USER root

# Script de démarrage pour le master
COPY start-master.sh /start-master.sh
RUN chmod +x /start-master.sh

USER hadoop

CMD ["/start-master.sh"]