#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'ingestion des données e-commerce vers MongoDB.
Ce script importe les événements depuis un fichier JSON vers la base MongoDB.
"""

import json
import os
import time
from pymongo import MongoClient, ASCENDING, DESCENDING

# Configuration MongoDB
MONGO_URI = "mongodb://admin:password@mongodb:27017/"
DB_NAME = "ecommerce"
COLLECTION_NAME = "events"

def connect_to_mongodb():
    """Établit une connexion à MongoDB avec retry en cas d'échec."""
    max_retries = 5
    for i in range(max_retries):
        try:
            client = MongoClient(MONGO_URI)
            print("Connexion à MongoDB réussie!")
            return client
        except Exception as e:
            if i < max_retries - 1:
                wait_time = 2 ** i  # Backoff exponentiel
                print(f"Échec de connexion. Nouvelle tentative dans {wait_time} secondes...")
                time.sleep(wait_time)
            else:
                print(f"Impossible de se connecter à MongoDB après {max_retries} tentatives.")
                raise e

def create_indexes(collection):
    """Création des index optimisés pour nos requêtes prévues."""
    # Index pour accès rapide par user_id
    collection.create_index([("user_id", ASCENDING)])
    
    # Index pour accès rapide par timestamp
    collection.create_index([("timestamp", DESCENDING)])
    
    # Index composé pour type d'événement et timestamp (pour analyse par période)
    collection.create_index([("event_type", ASCENDING), ("timestamp", DESCENDING)])
    
    # Index pour recherches par catégorie
    collection.create_index([("category", ASCENDING)])
    
    # Index pour région (pour analyses géographiques)
    collection.create_index([("region", ASCENDING)])
    
    print("Indexation terminée!")

def import_data(file_path, collection):
    """Importe les données JSON dans MongoDB."""
    with open(file_path, 'r') as file:
        documents = []
        for line in file:
            if line.strip():  # Ignore les lignes vides
                doc = json.loads(line)
                documents.append(doc)
                
                # Insertion par lots pour optimiser les performances
                if len(documents) >= 1000:
                    collection.insert_many(documents)
                    documents = []
                    
        # Insertion du reste des documents
        if documents:
            collection.insert_many(documents)
    
    print(f"Importation terminée! {collection.count_documents({})} documents importés.")

def main():
    # Connexion à MongoDB
    client = connect_to_mongodb()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Vérification si la collection existe déjà et contient des données
    if collection.count_documents({}) > 0:
        print(f"La collection {COLLECTION_NAME} contient déjà des données.")
        user_input = input("Voulez-vous supprimer et réimporter les données? (o/n): ")
        if user_input.lower() == 'o':
            collection.drop()
            print("Collection supprimée. Réimportation des données...")
        else:
            print("Importation annulée.")
            return
    
    # Chemin du fichier de données
    data_path = "/app/data/ecommerce_events.json"
    if not os.path.exists(data_path):
        print(f"Erreur: Le fichier {data_path} n'existe pas!")
        return
    
    # Import des données
    import_data(data_path, collection)
    
    # Création des index
    create_indexes(collection)
    
    # Affichage des statistiques
    print("\nStatistiques de la base de données:")
    print(f"Nombre total d'événements: {collection.count_documents({})}")
    for event_type in ["search", "click", "purchase"]:
        count = collection.count_documents({"event_type": event_type})
        print(f"Nombre d'événements de type '{event_type}': {count}")

if __name__ == "__main__":
    main()
