#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'analyse simplifié pour les données e-commerce
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
import json
from pymongo import MongoClient
from datetime import datetime

# Configuration
OUTPUT_DIR = '/app/data/results'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def connect_to_mongodb():
    """Connexion à MongoDB et récupération des données"""
    try:
        print("Connexion à MongoDB...")
        client = MongoClient("mongodb://admin:password@mongodb:27017/")
        db = client["ecommerce"]
        collection = db["events"]
        
        # Récupération des données
        cursor = collection.find({})
        data = list(cursor)
        print(f"Données récupérées: {len(data)} documents")
        
        # Conversion en DataFrame
        df = pd.DataFrame(data)
        return df, client
    except Exception as e:
        print(f"Erreur de connexion à MongoDB: {e}")
        sys.exit(1)

def average_clicks_before_purchase(df):
    """Calcul du nombre moyen de clics avant achat"""
    print("Analyse des clics avant achat...")
    
    # Conversion de timestamp en datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(['user_id', 'timestamp'])
    
    # Suivi des clics par utilisateur
    user_sequences = []
    current_user = None
    click_count = 0
    
    for _, row in df.iterrows():
        if current_user != row['user_id']:
            current_user = row['user_id']
            click_count = 0
            
        if row['event_type'] == 'click':
            click_count += 1
        elif row['event_type'] == 'purchase':
            user_sequences.append(click_count)
            click_count = 0
    
    # Calcul de la moyenne
    if not user_sequences:
        avg_clicks = 0
    else:
        avg_clicks = sum(user_sequences) / len(user_sequences)
    
    print(f"Nombre moyen de clics avant achat: {avg_clicks:.2f}")
    return avg_clicks

def top_searches_by_hour(df):
    """Analyse des termes de recherche les plus populaires par heure"""
    print("Analyse des recherches par heure...")
    
    # Conversion du timestamp et extraction de l'heure
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.hour
    
    # Filtre des événements de recherche
    searches = df[df['event_type'] == 'search']
    if searches.empty:
        print("Aucune recherche trouvée dans les données")
        return None
    
    # Analyse des termes de recherche par heure
    hourly_searches = searches.groupby(['hour', 'search_query']).size().reset_index(name='count')
    hourly_searches = hourly_searches.sort_values(['hour', 'count'], ascending=[True, False])
    
    # Récupérer le terme le plus recherché pour chaque heure
    top_searches = []
    for hour in range(24):
        hour_data = hourly_searches[hourly_searches['hour'] == hour]
        if not hour_data.empty:
            top_term = hour_data.iloc[0]
            top_searches.append({
                'hour': hour,
                'top_search': top_term['search_query'],
                'count': top_term['count']
            })
    
    top_df = pd.DataFrame(top_searches)
    
    # Création du graphique
    plt.figure(figsize=(12, 8))
    plt.bar(top_df['hour'], top_df['count'], color='skyblue')
    plt.title('Terme de recherche le plus populaire par heure')
    plt.xlabel('Heure de la journée')
    plt.ylabel('Nombre de recherches')
    plt.xticks(range(24))
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Ajout des annotations
    for _, row in top_df.iterrows():
        plt.text(row['hour'], row['count'], row['top_search'], 
                 ha='center', va='bottom', rotation=45, fontsize=8)
    
    # Sauvegarde
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/top_searches_by_hour.png")
    print(f"Graphique sauvegardé: {OUTPUT_DIR}/top_searches_by_hour.png")
    
    return top_df

def top_products_by_region(df):
    """Top 10 des produits les plus consultés par région"""
    print("Analyse des produits par région...")
    
    # Filtrer les événements de clic
    clicks = df[df['event_type'] == 'click']
    if clicks.empty:
        print("Aucun clic trouvé dans les données")
        return None
    
    # Comptage par région et produit
    product_clicks = clicks.groupby(['region', 'product_id', 'category']).size().reset_index(name='count')
    product_clicks = product_clicks.sort_values(['region', 'count'], ascending=[True, False])
    
    # Top 10 par région
    regions = df['region'].unique()
    result = {}
    
    for region in regions:
        region_data = product_clicks[product_clicks['region'] == region]
        top10 = region_data.head(10)
        result[region] = top10
        
        # Création du graphique pour cette région
        if not top10.empty:
            plt.figure(figsize=(12, 6))
            bars = plt.bar(top10['product_id'], top10['count'], 
                           color=sns.color_palette("husl", len(top10)))
            
            # Ajout des étiquettes
            for i, bar in enumerate(bars):
                plt.text(bar.get_x() + bar.get_width()/2, 
                         bar.get_height() + 0.3,
                         top10['category'].iloc[i], 
                         ha='center', va='bottom', rotation=45, fontsize=8)
            
            plt.title(f'Top 10 des produits consultés - Région {region}')
            plt.xlabel('ID du produit')
            plt.ylabel('Nombre de clics')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Sauvegarde
            plt.savefig(f"{OUTPUT_DIR}/top10_products_region_{region}.png")
            print(f"Graphique sauvegardé: {OUTPUT_DIR}/top10_products_region_{region}.png")
    
    return result

def save_to_mongodb(client, data, collection_name):
    """Sauvegarde des résultats dans MongoDB"""
    try:
        print(f"Sauvegarde des résultats dans la collection {collection_name}...")
        db = client["ecommerce"]
        collection = db[collection_name]
        
        # Nettoyage de la collection existante
        collection.delete_many({})
        
        # Conversion des données en format JSON
        if isinstance(data, dict):
            if all(isinstance(v, pd.DataFrame) for v in data.values()):
                # Dictionnaire de DataFrames
                json_data = {}
                for k, v in data.items():
                    json_data[k] = v.to_dict('records')
                collection.insert_one(json_data)
            else:
                # Dictionnaire simple
                collection.insert_one(data)
        elif isinstance(data, pd.DataFrame):
            # DataFrame
            records = data.to_dict('records')
            collection.insert_many(records)
        else:
            # Valeur simple
            collection.insert_one({"result": data})
        
        print(f"Données sauvegardées dans MongoDB: {collection_name}")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde dans MongoDB: {e}")

def main():
    """Fonction principale d'analyse"""
    print("Démarrage de l'analyse des données e-commerce...")
    
    # Connexion et récupération des données
    df, client = connect_to_mongodb()
    
    # Analyse 1: Clics moyens avant achat
    avg_clicks = average_clicks_before_purchase(df)
    save_to_mongodb(client, {"avg_clicks_before_purchase": avg_clicks}, "avg_clicks_result")
    
    # Analyse 2: Recherches par heure
    top_searches = top_searches_by_hour(df)
    if top_searches is not None:
        save_to_mongodb(client, top_searches, "top_searches_by_hour")
    
    # Analyse 3: Produits par région
    top_products = top_products_by_region(df)
    if top_products is not None:
        # Les résultats sont trop complexes pour MongoDB, on les sauvegarde juste en graphiques
        pass
    
    print("Analyse terminée avec succès!")

if __name__ == "__main__":
    main()
