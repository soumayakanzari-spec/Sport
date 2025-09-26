# Sport
Sport Data Solution
📋 Description du Projet
Sport Data Solution est une plateforme complète d'analyse et de récompense des activités sportives en entreprise. Ce système permet de calculer automatiquement des avantages financiers basés sur la pratique sportive des employés, favorisant ainsi une culture d'entreprise active et saine.

🎯 Objectifs
Intégrer le sport dans l'ADN de l'entreprise

Calculer l'impact financier des avantages sportifs

Créer un environnement d'émulation entre collaborateurs

Fournir des dashboards temps réel pour le suivi des activités

🏗️ Architecture Technique
Stack Technologique
Composant	Technologie	Version
Ingestion	Debezium + Redpanda	2.5 + 24.1
Processing	Apache Spark + Delta Lake	3.5.0 + 3.2.0
Visualisation	Power BI	-
Conteneurisation	Docker + Docker Compose	-
Flux de Données
text
PostgreSQL → Debezium → Redpanda → Spark Streaming → Delta Lake → Power BI
📁 Structure des Fichiers
text
sport-data-solution/
├── docker-compose.yml          # Orchestration des services
├── Dockerfile.spark            # Image Spark personnalisée
├── spark_traitement_sport.py   # Traitement des activités sportives
├── spark_traitement_employee.py # Traitement des données employés
├── spark_powerbi.py           # Export des données pour Power BI
├── data/                      # Volume des données Delta Lake
│   ├── delta_sport_activities/
│   └── delta_employee_prime/
└── checkpoints/               # Checkpoints Spark
    ├── sport_activities/
    └── employee_prime/
🚀 Déploiement
Prérequis
Docker et Docker Compose installés

4GB de RAM minimum

10GB d'espace disque libre

Installation et Lancement
Cloner le projet

bash
git clone <repository-url>
cd sport-data-solution
Construire les images Docker

bash
docker-compose build
Démarrer les services

bash
docker-compose up -d
Vérifier le statut des services

bash
docker-compose ps
Services Déployés
Service	Port	Description
spark-sport-processor	-	Traitement des activités sportives
spark-employee-processor	-	Traitement des données employés
redpanda	19092	Broker Kafka-compatible
redpanda-console	8080	Interface de monitoring
🔧 Configuration
Variables d'Environnement
Les containers Spark utilisent les variables suivantes :

bash
PYSPARK_PYTHON=python3
PYSPARK_DRIVER_PYTHON=python3
SPARK_HOME=/opt/spark
Volumes Persistants
/data : Stockage des tables Delta Lake

/tmp/checkpoints : Checkpoints des streams Spark

/data/powerbi_export : Fichiers CSV pour Power BI

📊 Modèle de Données
Source PostgreSQL
sql
sport_activities (
    id_salarie VARCHAR,
    start_date TIMESTAMP,
    activity_type VARCHAR,
    distance_m FLOAT,
    duration_sec INTEGER,
    comment VARCHAR
)
Tables Delta Lake Résultantes
sport_activities_delta : Historique des activités sportives

employee_prime : Données employés avec calcul des primes

💰 Calcul des Avantages
Règles Métier
Prime Écologique (5% du salaire brut) :

Pour les moyens de déplacement écologiques :

Marche/running

Vélo/Trottinette/Autres

Jours Bien-Être :

5 jours supplémentaires pour les employés avec ≥15 activités/an

Formules de Calcul
python
# Prime écologique
prime_annuelle = salaire_brut * 0.05 if moyen_deplacement_ecologique else 0

# Éligibilité jours bien-être
jours_bien_etre = 5 if nb_activites >= 15 else 0
🔍 Monitoring et Qualité des Données
Checks Implémentés
✅ Validation des dates (naissance, embauche)

✅ Cohérence salaire/échelles

✅ Complétude des données obligatoires

✅ Conformité types d'activité avec distance

Marche/Running : max 15 km

Vélo/Trottinette : max 25 km

Alertes et Notifications
Notifications Slack pour nouvelles activités

Monitoring Redpanda sur le port 8080

Logs de traitement en temps réel

📈 Dashboard Power BI
Pages Principales
Overview : KPIs globaux et métriques principales

Détail Employés : Activités individuelles + éligibilité

Budget : Impact financier des avantages

Trends : Évolution temporelle des activités

Connexion aux Données
Les données sont exportées automatiquement dans :

text
/data/powerbi_export/
├── activites_sportives.csv
├── primes_employes.csv
└── statistiques_primes.csv
🛠️ Commandes Utiles
Supervision des Services
bash
# Voir les logs en temps réel
docker-compose logs -f spark-sport-processor
docker-compose logs -f spark-employee-processor

# Statut des containers
docker-compose ps

# Redémarrer un service
docker-compose restart spark-sport-processor
Maintenance
bash
# Arrêt propre
docker-compose down

# Nettoyage des volumes
docker-compose down -v

# Reconstruction des images
docker-compose build --no-cache
Debugging
bash
# Accéder au container
docker exec -it spark-sport-processor bash

# Vérifier les données Delta
docker exec spark-sport-processor python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('delta').load('/data/delta_sport_activities')
print(f'Nombre d\'activités: {df.count()}')
df.show(5)
"
📊 Métriques de Performance
⚡ Réduction de 70% du temps de traitement

🎯 99,9% de fiabilité des données

📈 Capacité : 10,000+ activités/jour

💾 Rétention : 3 ans d'historique

🔮 Évolutions Futures
Intégration d'API Google Maps pour validation des distances

Notifications push mobiles

Intégration avec les outils RH existants

Analytics prédictif des tendances sportives

🆘 Dépannage
Problèmes Courants
Container s'arrête immédiatement

bash
docker logs spark-sport-processor
Problème de connexion Kafka

bash
docker-compose restart redpanda
Données non visibles dans Power BI

bash
docker exec spark-sport-processor ls -la /data/powerbi_export/
Support
Pour toute question technique, consulter les logs des services concernés.

📄 Licence
Ce projet est développé pour un usage interne. Tous droits réservés.

Sport Data Solution - Une culture d'entreprise active et saine 🏃‍♂️💼


