FROM python:3.9

WORKDIR /app

# Installation des dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie des scripts dans le conteneur
COPY scripts/ /app/scripts/
COPY data/ /app/data/

# Création d'un script pour exécuter l'ingestion puis rester actif
RUN echo '#!/bin/bash\npython /app/scripts/ingest_data.py\necho "Données ingérées - Conteneur maintenant en attente"\ntail -f /dev/null' > /app/entrypoint.sh \
    && chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
