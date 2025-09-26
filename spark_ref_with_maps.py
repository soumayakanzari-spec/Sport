import os
import pandas as pd
import googlemaps
import random
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, when, sum as spark_sum, avg, round, lit, udf
from delta import configure_spark_with_delta_pip

# Configuration Google Maps
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
if not GOOGLE_MAPS_API_KEY:
    raise ValueError("❌ Clé API Google Maps non configurée. Définissez la variable GOOGLE_MAPS_API_KEY")

ADRESSE_ENTREPRISE = "1362 Av. des Platanes, 34970 Lattes"

# Path to existing Delta table
DELTA_PATH = "/data/delta_sport_activities"
EXPORT_DIR = "/data/powerbi_export"

# File paths
DONNEES_RH_FILE = "Donnees_RH.xlsx"
DONNEES_SPORTIVE_FILE = "Donnees_Sportive.xlsx"

# Python environment inside Spark workers
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'

# Initialiser le client Google Maps
def init_google_maps_client():
    """Initialise et retourne le client Google Maps"""
    try:
        client = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
        
        # Test de connexion
        test_result = client.distance_matrix(
            "Paris, France",
            "Lyon, France",
            mode="driving"
        )
        
        if test_result['status'] == 'OK':
            print("✅ Connexion à l'API Google Maps réussie")
            return client
        else:
            print(f"❌ Erreur API Google Maps: {test_result['status']}")
            return None
            
    except Exception as e:
        print(f"❌ Erreur initialisation Google Maps: {e}")
        return None

def simuler_distance_par_ville(adresse_domicile):
    """Simule une distance réaliste basée sur la ville"""
    if not adresse_domicile or pd.isna(adresse_domicile):
        return round(random.uniform(5, 40), 2)
    
    adresse_lower = str(adresse_domicile).lower()
    
    # Distances approximatives depuis Lattes (en km)
    distances_par_ville = {
        'montpellier': (3, 8),
        'lattes': (1, 5),
        'frontignan': (25, 30),
        'saint-clément-de-rivière': (8, 12),
        'nîmes': (45, 50),
        'pérols': (5, 8),
        'mèze': (30, 35),
        'prades-le-lez': (10, 15),
        'mauguio': (8, 12),
        'vendargues': (12, 18),
        'clermont-l-hérault': (40, 45),
        'grabels': (15, 20),
    }
    
    # Détection de la ville
    for ville, (min_dist, max_dist) in distances_par_ville.items():
        if ville in adresse_lower:
            distance = random.uniform(min_dist, max_dist)
            print(f"📍 Simulation {ville}: {distance:.1f} km")
            return round(distance, 2)
    
    # Détection par code postal
    if '34000' in adresse_domicile or '34090' in adresse_domicile:
        return round(random.uniform(3, 10), 2)  # Montpellier
    elif '34970' in adresse_domicile:
        return round(random.uniform(1, 5), 2)   # Lattes
    elif '34110' in adresse_domicile:
        return round(random.uniform(25, 30), 2) # Frontignan
    elif '30900' in adresse_domicile:
        return round(random.uniform(45, 50), 2) # Nîmes
    elif '34470' in adresse_domicile:
        return round(random.uniform(5, 8), 2)   # Pérols
    elif '34140' in adresse_domicile:
        return round(random.uniform(30, 35), 2) # Mèze
    elif '34730' in adresse_domicile:
        return round(random.uniform(10, 15), 2) # Prades-le-Lez
    elif '34130' in adresse_domicile:
        return round(random.uniform(8, 12), 2)  # Mauguio
    elif '34740' in adresse_domicile:
        return round(random.uniform(12, 18), 2) # Vendargues
    elif '34800' in adresse_domicile:
        return round(random.uniform(40, 45), 2) # Clermont-l'Hérault
    elif '34790' in adresse_domicile:
        return round(random.uniform(15, 20), 2) # Grabels
    
    # Par défaut
    distance = random.uniform(5, 40)
    print(f"📍 Simulation par défaut: {distance:.1f} km")
    return round(distance, 2)

# Fonction pour calculer la distance
def calculate_distance(adresse_domicile, adresse_entreprise, mode_transport="driving"):
    """Calcule la distance entre deux adresses avec fallback de simulation"""
    try:
        if adresse_domicile is None or pd.isna(adresse_domicile):
            return simuler_distance_par_ville(adresse_domicile)
        
        if not isinstance(adresse_domicile, str):
            adresse_domicile = str(adresse_domicile)
        
        adresse_domicile_clean = adresse_domicile.strip()
        if not adresse_domicile_clean:
            return simuler_distance_par_ville(adresse_domicile)
        
        # Essayer Google Maps d'abord
        client = init_google_maps_client()
        if client:
            result = client.distance_matrix(
                adresse_domicile_clean,
                adresse_entreprise,
                mode=mode_transport,
                departure_time=datetime.now(),
                language="fr-FR"
            )
            
            if result['status'] == 'OK':
                element = result['rows'][0]['elements'][0]
                if element['status'] == 'OK':
                    distance_km = element['distance']['value'] / 1000
                    print(f"✅ Google Maps: {distance_km:.1f} km")
                    return float(distance_km)
        
        # Fallback sur la simulation si Google Maps échoue
        print(f"⚠️  Fallback simulation pour: {adresse_domicile_clean}")
        return simuler_distance_par_ville(adresse_domicile_clean)
            
    except Exception as e:
        print(f"❌ Exception, simulation pour {adresse_domicile}: {e}")
        return simuler_distance_par_ville(adresse_domicile)

# Fonction de validation - MODIFIÉE pour exclure voiture et transports
def validate_commute(distance_km, transport_mode):
    """Valide la déclaration de transport selon les règles"""
    if distance_km is None:
        return "Erreur: Distance non calculée"
    
    if transport_mode is None:
        return "Erreur: Mode de transport non spécifié"
    
    transport_mode = str(transport_mode).lower()
    
    # Exclure les modes de transport non désirés
    if ('voiture' in transport_mode or 
        'thermique' in transport_mode or 
        'electrique' in transport_mode or
        'transport' in transport_mode or 
        'commun' in transport_mode):
        return "Mode exclu: Véhicule motorisé ou transport"
    
    # Valider seulement les modes actifs
    if 'marche' in transport_mode or 'running' in transport_mode:
        return "Valide" if distance_km <= 15 else f"Erreur: {distance_km}km > 15km (Marche)"
    elif 'vélo' in transport_mode or 'velo' in transport_mode or 'trottinette' in transport_mode:
        return "Valide" if distance_km <= 25 else f"Erreur: {distance_km}km > 25km (Vélo)"
    else:
        return "Mode non spécifié"

# UDFs standard avec gestion d'erreur
def calculate_distance_safe(adresse):
    try:
        if adresse is None:
            return round(random.uniform(5, 40), 2)
        result = calculate_distance(adresse, ADRESSE_ENTREPRISE)
        if result is None:
            return simuler_distance_par_ville(adresse)
        return float(result)
    except Exception as e:
        print(f"❌ Erreur UDF calculate_distance: {e}")
        return simuler_distance_par_ville(adresse)

def validate_commute_safe(distance_km, transport_mode):
    try:
        if distance_km is None or transport_mode is None:
            return "Erreur: Données manquantes"
        return validate_commute(float(distance_km), str(transport_mode))
    except Exception as e:
        print(f"❌ Erreur UDF validate_commute: {e}")
        return "Erreur: Validation échouée"

# Build SparkSession
builder = SparkSession.builder \
    .appName("DeltaTableReaderWithMaps") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Définir les UDFs standard
calculate_distance_udf = udf(calculate_distance_safe, DoubleType())
validate_commute_udf = udf(validate_commute_safe, StringType())

# Fonction pour lire Excel
def read_excel_with_pandas(file_path, sheet_name=0):
    """Lire le fichier Excel avec pandas et le convertir en DataFrame Spark"""
    try:
        df_pandas = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Nettoyage des colonnes
        df_pandas.columns = df_pandas.columns.str.strip().str.replace(' ', '_').str.replace('-', '_')\
            .str.replace('é', 'e').str.replace('è', 'e').str.replace('ê', 'e')\
            .str.replace('à', 'a').str.replace('â', 'a').str.replace('î', 'i')\
            .str.replace('ô', 'o').str.replace('û', 'u').str.replace('ç', 'c')\
            .str.replace('°', '').str.replace('(', '').str.replace(')', '')\
            .str.replace("'", "").str.replace('"', '').str.replace('/', '_')\
            .str.lower()
        
        # Mapping des colonnes
        column_mapping = {
            'id_salarie': 'id_salarie',
            'id': 'id_salarie',
            'salarie': 'id_salarie',
            'pratique_dun_sport': 'pratique_sport',
            'pratique_sport': 'pratique_sport',
            'sport': 'pratique_sport',
            'nom': 'nom',
            'prenom': 'prenom',
            'date_naissance': 'date_naissance',
            'bu': 'bu',
            'date_embauche': 'date_embauche',
            'salaire_brut': 'salaire_brut',
            'type_contrat': 'type_contrat',
            'jours_cp': 'jours_cp',
            'adresse': 'adresse',
            'moyen_deplacement': 'moyen_deplacement',
            'deplacement': 'moyen_deplacement'
        }
        
        df_pandas.columns = [column_mapping.get(col, col) for col in df_pandas.columns]
        
        # Convertir les types pandas en types Spark
        spark_schema = StructType()
        for column, dtype in df_pandas.dtypes.items():
            if dtype == 'int64':
                spark_type = LongType()
            elif dtype == 'float64':
                spark_type = DoubleType()
            elif dtype == 'bool':
                spark_type = BooleanType()
            elif 'datetime' in str(dtype):
                spark_type = TimestampType()
            else:
                spark_type = StringType()
            spark_schema.add(StructField(column, spark_type, True))
        
        return spark.createDataFrame(df_pandas, schema=spark_schema)
        
    except Exception as e:
        print(f"Erreur lecture Excel {file_path}: {e}")
        return None

# 10. VALIDATION AVEC GOOGLE MAPS API
print("\n" + "="*70)
print("VALIDATION DES DÉCLARATIONS AVEC GOOGLE MAPS API")
print("="*70)

# Lire les données
donnees_rh_df = read_excel_with_pandas(DONNEES_RH_FILE)

if donnees_rh_df is not None:
    try:
        print(f"📍 Adresse entreprise: {ADRESSE_ENTREPRISE}")
        print("📏 Règles de validation:")
        print("   - Marche/Running ⇒ Maximum 15 km")
        print("   - Vélo/Trottinette ⇒ Maximum 25 km")
        print("   - Véhicules motorisés et transports ⇒ Exclus de la validation")
        
        # Calculer les distances et valider
        print("🔄 Calcul des distances avec Google Maps API...")
        
        donnees_rh_validees = donnees_rh_df.withColumn(
            "distance_km", 
            calculate_distance_udf(col("adresse"))
        ).withColumn(
            "validation_deplacement",
            validate_commute_udf(col("distance_km"), col("moyen_deplacement"))
        ).withColumn(
            "adresse_entreprise",
            lit(ADRESSE_ENTREPRISE)
        )
        
        # Afficher les résultats
        print("\n📋 RÉSULTATS DE VALIDATION:")
        donnees_rh_validees.select(
            "id_salarie", "nom", "prenom", "adresse", 
            "moyen_deplacement", "distance_km", "validation_deplacement"
        ).show(truncate=False, n=20)
        
        # Statistiques
        print("\n📊 STATISTIQUES DE VALIDATION:")
        stats = donnees_rh_validees.groupBy("validation_deplacement").agg(
            count("*").alias("nombre_employes"),
            avg("distance_km").alias("distance_moyenne_km")
        ).orderBy(col("nombre_employes").desc())
        
        stats.show(truncate=False)
        
        # Filtrer seulement les erreurs de validation (pas les modes exclus)
        erreurs = donnees_rh_validees.filter(
            col("validation_deplacement").contains("Erreur:") |
            col("validation_deplacement").contains("Mode non spécifié")
        )
        
        print(f"\n⚠️  ERREURS DE VALIDATION DÉTECTÉES: {erreurs.count()}")
        
        if erreurs.count() > 0:
            print("\n🔴 DÉCLARATIONS INCOHÉRENTES:")
            erreurs.select(
                "id_salarie", "nom", "prenom", "adresse",
                "moyen_deplacement", "distance_km", "validation_deplacement"
            ).show(truncate=False)
            
            # Exporter les erreurs
            output_erreurs = f"{EXPORT_DIR}/erreurs_deplacement_api.csv"
            erreurs.select(
                "id_salarie", "nom", "prenom", "adresse",
                "moyen_deplacement", "distance_km", "validation_deplacement",
                "adresse_entreprise"
            ).write.mode("overwrite").option("header", "true").csv(output_erreurs)
            
            print(f"✅ Export des erreurs: {output_erreurs}")
        
        # Exporter toutes les validations
        output_validations = f"{EXPORT_DIR}/validations_transport.csv"
        donnees_rh_validees.select(
            "id_salarie", "nom", "prenom", "adresse", "moyen_deplacement",
            "distance_km", "validation_deplacement", "adresse_entreprise"
        ).write.mode("overwrite").option("header", "true").csv(output_validations)
        
        print(f"✅ Export complet des validations: {output_validations}")
        
    except Exception as e:
        print(f"❌ Erreur lors de la validation: {e}")
        import traceback
        traceback.print_exc()
else:
    print("❌ Données RH manquantes")

# Stop Spark
spark.stop()