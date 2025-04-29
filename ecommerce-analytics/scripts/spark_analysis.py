#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script d'analyse des données e-commerce avec PySpark.
Répond aux questions analytiques demandées dans l'examen.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, desc, window
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os

# Configuration de l'environnement
OUTPUT_DIR = '/app/data/results'

def create_spark_session():
    """Crée et retourne une session Spark configurée avec MongoDB."""
    return (SparkSession.builder
            .appName("ECommerceAnalysis")
            .config("spark.mongodb.input.uri", "mongodb://admin:password@mongodb:27017/ecommerce.events")
            .config("spark.mongodb.output.uri", "mongodb://admin:password@mongodb:27017/ecommerce.results")
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
            .getOrCreate())

def load_data(spark):
    """Charge les données depuis MongoDB dans un DataFrame Spark."""
    return spark.read.format("mongo").load()

def average_clicks_before_purchase(df):
    """
    Calcule le nombre moyen de clics avant un achat.
    """
    # On regroupe les événements par user_id
    user_events = df.select("user_id", "event_type", "timestamp", "product_id") \
                    .orderBy("user_id", "timestamp")
    
    # Conversion en pandas pour l'analyse séquentielle
    pdf = user_events.toPandas()
    
    # Analyse du comportement par utilisateur
    click_counts = []
    current_user = None
    current_clicks = 0
    
    for index, row in pdf.iterrows():
        if current_user != row['user_id']:
            # Nouvel utilisateur
            current_user = row['user_id']
            current_clicks = 0
        
        if row['event_type'] == 'click':
            current_clicks += 1
        elif row['event_type'] == 'purchase':
            # Achat détecté, on enregistre le nombre de clics précédents
            click_counts.append(current_clicks)
            current_clicks = 0
    
    # Calcul de la moyenne
    avg_clicks = sum(click_counts) / len(click_counts) if click_counts else 0
    
    print(f"Nombre moyen de clics avant achat : {avg_clicks:.2f}")
    return avg_clicks

def top_categories_by_hour(df):
    """
    Identifie les catégories les plus recherchées par heure.
    """
    # Ajout d'une colonne 'hour' pour l'heure du timestamp
    df_with_hour = df.withColumn("hour", hour(col("timestamp")))
    
    # Filtrer uniquement les recherches et compter par catégorie et heure
    searches = df_with_hour.filter(col("event_type") == "search")
    
    # Compter les recherches par query et heure
    search_counts = searches.groupBy("hour", "search_query") \
                            .count() \
                            .orderBy("hour", desc("count"))
    
    # Prendre le top pour chaque heure
    top_searches_by_hour = search_counts.groupBy("hour") \
                                        .agg({"search_query": "first", "count": "max"}) \
                                        .withColumnRenamed("first(search_query)", "top_search") \
                                        .withColumnRenamed("max(count)", "search_count") \
                                        .orderBy("hour")
    
    # Affichage des résultats
    top_searches_by_hour.show(24, False)
    
    # Conversion en pandas pour visualisation
    pdf = top_searches_by_hour.toPandas()
    
    # Création du graphique
    plt.figure(figsize=(12, 8))
    plt.bar(pdf['hour'], pdf['search_count'], color='skyblue')
    plt.title('Nombre de recherches pour la catégorie la plus populaire par heure')
    plt.xlabel('Heure de la journée')
    plt.ylabel('Nombre de recherches')
    plt.xticks(range(24))
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Ajout des annotations avec les termes de recherche les plus populaires
    for i, row in pdf.iterrows():
        plt.text(row['hour'], row['search_count'], row['top_search'], 
                 ha='center', va='bottom', rotation=45, fontsize=8)
    
    # Sauvegarde du graphique
    plt.tight_layout()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    plt.savefig(f"{OUTPUT_DIR}/top_searches_by_hour.png")
    
    return top_searches_by_hour

def top_products_by_region(df):
    """
    Génère un top 10 des produits les plus consultés par région.
    """
    # Filtrer uniquement les clics et compter par produit et région
    clicks = df.filter(col("event_type") == "click")
    
    # Compter les clics par produit et région
    product_clicks = clicks.groupBy("region", "product_id", "category") \
                          .count() \
                          .orderBy("region", desc("count"))
    
    # Obtenir le top 10 pour chaque région
    top_products = {}
    for region in df.select("region").distinct().collect():
        region_name = region["region"]
        top10 = product_clicks.filter(col("region") == region_name) \
                             .limit(10) \
                             .toPandas()
        top_products[region_name] = top10
    
    # Création de visualisations pour chaque région
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for region, data in top_products.items():
        plt.figure(figsize=(12, 6))
        
        # Création des barres avec couleurs par catégorie
        bars = plt.bar(data['product_id'], data['count'], color=sns.color_palette("husl", len(data)))
        
        # Ajout des étiquettes de catégorie
        for i, bar in enumerate(bars):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                    data['category'].iloc[i], ha='center', va='bottom', 
                    rotation=45, fontsize=8)
        
        plt.title(f'Top 10 des produits les plus consultés - Région {region}')
        plt.xlabel('ID du produit')
        plt.ylabel('Nombre de clics')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Sauvegarde de l'image
        plt.savefig(f"{OUTPUT_DIR}/top10_products_region_{region}.png")
    
    # Affichage des résultats
    for region, data in top_products.items():
        print(f"\nTop 10 des produits pour la région '{region}':")
        print(data[['product_id', 'category', 'count']])
    
    return top_products

def save_results_to_mongodb(results, collection_name):
    """
    Sauvegarde les résultats de l'analyse dans MongoDB.
    """
    # Conversion en JSON pour MongoDB
    if isinstance(results, dict):
        results_json = {}
        for key, value in results.items():
            if isinstance(value, pd.DataFrame):
                results_json[key] = value.to_dict('records')
            else:
                results_json[key] = value
    elif isinstance(results, pd.DataFrame):
        results_json = results.to_dict('records')
    else:
        results_json = {"result": results}
    
    # Connexion à MongoDB
    from pymongo import MongoClient
    client = MongoClient("mongodb://admin:password@mongodb:27017/")
    db = client["ecommerce"]
    collection = db[collection_name]
    
    # Sauvegarde des résultats
    collection.insert_many(results_json) if isinstance(results_json, list) else collection.insert_one(results_json)
    
    print(f"Résultats sauvegardés dans la collection '{collection_name}'")

def main():
    """Fonction principale d'exécution du script d'analyse."""
    print("Démarrage de l'analyse Spark...")
    
    # Création de la session Spark
    spark = create_spark_session()
    
    # Chargement des données
    df = load_data(spark)
    print(f"Données chargées: {df.count()} événements")
    
    # Analyse 1: Nombre moyen de clics avant achat
    avg_clicks = average_clicks_before_purchase(df)
    save_results_to_mongodb({"avg_clicks_before_purchase": avg_clicks}, "avg_clicks_result")
    
    # Analyse 2: Catégories les plus recherchées par heure
    top_categories = top_categories_by_hour(df)
    # Les résultats sont déjà sauvegardés en image
    
    # Analyse 3: Top 10 des produits les plus consultés par région
    top_products = top_products_by_region(df)
    # Les résultats sont déjà sauvegardés en image
    
    print("Analyse terminée avec succès!")
    
    # Arrêt de la session Spark
    spark.stop()

if __name__ == "__main__":
    main()
