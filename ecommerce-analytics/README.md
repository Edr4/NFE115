# Analyse de Données E-commerce avec MongoDB et Spark

Ce projet implémente une solution d'analyse de données pour une plateforme e-commerce, en utilisant MongoDB comme base NoSQL et Spark pour l'analyse distribuée. L'architecture est conteneurisée avec Docker pour faciliter le déploiement.

## Architecture de la Solution

La solution comporte trois composants principaux, tous orchestrés via Docker Compose :

1. **MongoDB** : Base de données NoSQL pour stocker les événements utilisateurs
2. **Spark** : Moteur de traitement distribué pour l'analyse des données
3. **Application Python** : Scripts d'ingestion et d'analyse

## Structure du Projet

```
ecommerce-analytics/
├── docker-compose.yml      # Configuration des services Docker
├── Dockerfile              # Configuration du conteneur d'application
├── requirements.txt        # Dépendances Python
├── data/                   # Stockage des données et résultats
│   └── ecommerce_events.json  # Données brutes d'événements
├── scripts/                # Scripts d'analyse et d'ingestion
│   ├── ingest_data.py      # Importation des données dans MongoDB
│   └── spark_analysis.py   # Analyse Spark
└── README.md               # Documentation
```

## Étape 1 : Modélisation NoSQL avec MongoDB

### Choix de Modélisation

- **Base de données** : `ecommerce`
- **Collection** : `events`
- **Modèle de document** : Document JSON représentant un événement utilisateur

### Justification des Choix

- **Collection unique** : Facilite l'analyse complète du parcours utilisateur en gardant tous les événements ensemble
- **Indexation** :
  - Index sur `user_id` : Accès rapide par utilisateur
  - Index sur `timestamp` : Accès rapide par date/heure
  - Index composé sur `(event_type, timestamp)` : Pour l'analyse par type d'événement sur une période
  - Index sur `region` : Pour l'analyse par région
  - Index sur `category` : Pour l'analyse par catégorie de produit

### Requêtes Optimisées

1. **Requête par utilisateur** :
   ```javascript
   db.events.find({ user_id: "U1234" }).sort({ timestamp: -1 })
   ```

2. **Requête par date** :
   ```javascript
   db.events.find({ 
     timestamp: { 
       $gte: ISODate("2025-04-01T00:00:00Z"), 
       $lt: ISODate("2025-04-02T00:00:00Z") 
     }
   }).sort({ timestamp: 1 })
   ```

## Étape 2 : Analyse avec Spark

Le script `spark_analysis.py` réalise trois analyses principales :

1. **Nombre moyen de clics avant achat** : Analyse séquentielle des actions utilisateurs pour déterminer combien de produits sont consultés avant de procéder à un achat.

2. **Catégories les plus recherchées par heure** : Identification des tendances de recherche selon l'heure de la journée.

3. **Top 10 des produits par région** : Analyse des produits les plus populaires dans chaque région géographique.

## Utilisation

1. Démarrer l'environnement Docker :
   ```bash
   docker-compose up -d
   ```

2. Importer les données dans MongoDB :
   ```bash
   docker-compose exec app python /app/scripts/ingest_data.py
   ```

3. Exécuter l'analyse Spark :
   ```bash
   docker-compose exec app python /app/scripts/spark_analysis.py
   ```

4. Les résultats sont sauvegardés dans :
   - MongoDB dans la base `ecommerce`, collections `avg_clicks_result` et autres
   - Images dans le dossier `data/results/`

## Performance et Mise à l'échelle

- Les index MongoDB sont optimisés pour les types de requêtes fréquentes
- L'utilisation de Spark permet un traitement distribué pour des volumes plus importants
- L'architecture conteneurisée facilite le déploiement sur plusieurs nœuds

---

*Projet réalisé dans le cadre du module "Gestion des Données à Large Échelle & Bases NoSQL"*
