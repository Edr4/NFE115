#!/bin/bash
# Script d'automatisation pour l'ingestion et l'analyse des données

echo "Démarrage de l'analyse de données e-commerce..."

# 1. Ingestion des données
echo "Étape 1: Ingestion des données dans MongoDB..."
python /app/scripts/ingest_data.py

# 2. Analyse avec Spark
echo "Étape 2: Exécution de l'analyse Spark..."
python /app/scripts/spark_analysis.py

echo "Analyse terminée! Les résultats sont disponibles dans /app/data/results/"
echo "Vous pouvez également consulter les résultats dans la base MongoDB 'ecommerce'"
