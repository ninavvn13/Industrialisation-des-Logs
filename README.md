# Stockage et Indexation des Logs – Elasticsearch

## Partie 2.3

## 1. Choix de la base
Elasticsearch est utilisé pour :
- Indexer les logs JSON
- Effectuer des recherches rapides
- Gérer un grand volume de données
- Visualiser via Kibana

## 2. Partitionnement
Les logs sont stockés dans un index quotidien nommé `logs-YYYY.MM.DD`.

## 3. Rétention
Une politique ILM supprime automatiquement les index après 30 jours.

## 4. Optimisations
- `keyword` : pour les champs à filtrer
- `ip` : pour les adresses IP
- `date` : pour les timestamps

## 5. Commandes `curl` pour configurer Elasticsearch

### 5.1. Ajouter la politique ILM (Index Lifecycle Management)

Pour ajouter la politique ILM qui gère la rétention des données, utilise la commande suivante :

curl -X PUT "http://localhost:9200/_ilm/policy/logs_policy" -H "Content-Type: application/json" -d @logs_policy.json

### 5.2 Ajouter le template d’index (mapping + paramètres)

curl -X PUT "http://localhost:9200/_index_template/logs-template" -H "Content-Type: application/json" -d @index_mapping.json

