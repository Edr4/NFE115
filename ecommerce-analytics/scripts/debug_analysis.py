#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de débogage pour l'analyse e-commerce
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
import traceback
from pymongo import MongoClient

# Affichage détaillé pour le débogage
def debug_print(message):
    print(f"DEBUG: {message}")
    sys.stdout.flush()  # Force l'affichage immédiat

# Configuration
OUTPUT_DIR = '/app/data/results'

def main():
    try:
        debug_print("Démarrage du script de débogage...")
        debug_print(f"Python version: {sys.version}")
        
        # Vérification du répertoire de résultats
        debug_print(f"Création du répertoire: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Test d'écriture de fichier
        test_file = f"{OUTPUT_DIR}/test.txt"
        debug_print(f"Test d'écriture dans {test_file}")
        with open(test_file, 'w') as f:
            f.write("Test d'écriture réussi!")
        debug_print("Écriture réussie!")
        
        # Test de connexion MongoDB
        debug_print("Connexion à MongoDB...")
        client = MongoClient("mongodb://admin:password@mongodb:27017/", serverSelectionTimeoutMS=5000)
        debug_print("Vérification de la connexion à MongoDB...")
        client.admin.command('ping')  # Vérifie que la connexion fonctionne
        debug_print("Connexion à MongoDB réussie!")
        
        # Accès à la base de données
        db = client["ecommerce"]
        collection = db["events"]
        count = collection.count_documents({})
        debug_print(f"Nombre de documents dans la collection events: {count}")
        
        # Récupération de quelques événements
        debug_print("Récupération de 5 événements...")
        sample_events = list(collection.find().limit(5))
        for i, event in enumerate(sample_events):
            debug_print(f"Événement {i+1}: {event}")
        
        # Analyse simple (uniquement pour le débogage)
        debug_print("Calcul des statistiques simples...")
        
        # Compter par type d'événement
        event_types = collection.aggregate([
            {"$group": {"_id": "$event_type", "count": {"$sum": 1}}}
        ])
        for event_type in event_types:
            debug_print(f"Type d'événement: {event_type['_id']}, Nombre: {event_type['count']}")
        
        # Test de création d'un graphique simple
        debug_print("Création d'un graphique simple...")
        plt.figure(figsize=(6, 4))
        plt.bar([1, 2, 3], [4, 6, 2])
        plt.title("Test de graphique")
        plt.savefig(f"{OUTPUT_DIR}/test_chart.png")
        debug_print(f"Graphique sauvegardé dans {OUTPUT_DIR}/test_chart.png")
        
        # Test d'écriture dans MongoDB
        debug_print("Test d'écriture dans MongoDB...")
        test_collection = db["test_results"]
        test_collection.delete_many({})  # Nettoyage
        test_collection.insert_one({"test": "succès", "timestamp": pd.Timestamp.now().isoformat()})
        debug_print("Écriture dans MongoDB réussie!")
        
        debug_print("Script de débogage terminé avec succès!")
        return 0
        
    except Exception as e:
        debug_print(f"ERREUR: {str(e)}")
        debug_print("Traceback complet:")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
