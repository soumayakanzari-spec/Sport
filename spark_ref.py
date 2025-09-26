import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, when, sum as spark_sum, avg, round
from delta import configure_spark_with_delta_pip

# Path to existing Delta tables
DELTA_PATH = "/data/delta_sport_activities"
EMPLOYEE_PRIME_PATH = "/data/delta_employee_prime"

# File paths
DONNEES_RH_FILE = "Donnees_RH.xlsx"
DONNEES_SPORTIVE_FILE = "Donnees_Sportive.xlsx"

# Python environment inside Spark workers
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'

# Build SparkSession with Delta ONLY
builder = SparkSession.builder \
    .appName("DeltaTableReader") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Fonction pour lire Excel avec gestion robuste des colonnes
def read_excel_with_pandas(file_path, sheet_name=0):
    """Lire le fichier Excel avec pandas et le convertir en DataFrame Spark"""
    try:
        # Lire avec pandas
        df_pandas = pd.read_excel(file_path, sheet_name=sheet_name)
        
        print(f"Colonnes originales dans {file_path}: {list(df_pandas.columns)}")
        
        # Nettoyer les noms de colonnes
        df_pandas.columns = df_pandas.columns.str.strip().str.replace(' ', '_').str.replace('-', '_')\
            .str.replace('é', 'e').str.replace('è', 'e').str.replace('ê', 'e')\
            .str.replace('à', 'a').str.replace('â', 'a').str.replace('î', 'i')\
            .str.replace('ô', 'o').str.replace('û', 'u').str.replace('ç', 'c')\
            .str.replace('°', '').str.replace('(', '').str.replace(')', '')\
            .str.replace("'", "").str.replace('"', '').str.replace('/', '_')\
            .str.lower()  # Tout en minuscules pour plus de robustesse
        
        print(f"Colonnes nettoyées: {list(df_pandas.columns)}")
        
        # Mapping spécifique pour les colonnes connues
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
        
        # Renommer les colonnes selon le mapping
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
        
        # Créer le DataFrame Spark
        df_spark = spark.createDataFrame(df_pandas, schema=spark_schema)
        print(f"Schéma Spark final: {df_spark.schema}")
        return df_spark
        
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Excel {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return None

# Lire les fichiers Excel
print("Lecture des fichiers Excel...")
donnees_rh_df = read_excel_with_pandas(DONNEES_RH_FILE)
donnees_sportive_df = read_excel_with_pandas(DONNEES_SPORTIVE_FILE)

# Afficher les données
if donnees_rh_df is not None:
    print("\n===== DONNÉES RH =====")
    donnees_rh_df.show(5, truncate=False)
    print("Colonnes RH:", donnees_rh_df.columns)

if donnees_sportive_df is not None:
    print("\n===== DONNÉES SPORTIVES =====")
    donnees_sportive_df.show(5, truncate=False)
    print("Colonnes Sportives:", donnees_sportive_df.columns)

# Lire les tables Delta
try:
    print("\nLecture de la table Delta sport_activities...")
    df_delta = spark.read.format("delta").load(DELTA_PATH)
    print(f"Nombre d'activités dans Delta: {df_delta.count()}")
    print("Colonnes Delta:", df_delta.columns)
    df_delta.show(5, truncate=False)
except Exception as e:
    print(f"Erreur lors de la lecture de la table Delta sport_activities: {e}")
    df_delta = None

# Nouveau: Lecture de la table employee_prime
try:
    print("\nLecture de la table Delta employee_prime...")
    df_employee_prime = spark.read.format("delta").load(EMPLOYEE_PRIME_PATH)
    print(f"Nombre d'employés dans employee_prime: {df_employee_prime.count()}")
    print("Colonnes employee_prime:", df_employee_prime.columns)
    df_employee_prime.show(5, truncate=False)
    
    # Vérifier si les colonnes nécessaires existent (en minuscules maintenant)
    required_columns = ["id_salarie", "salaire_brut", "moyen_deplacement", "prime_annuelle"]
    for col_name in required_columns:
        if col_name not in df_employee_prime.columns:
            print(f"ATTENTION: La colonne {col_name} n'existe pas dans employee_prime")
    
    # Si prime_annuelle n'existe pas, la calculer
    if "prime_annuelle" not in df_employee_prime.columns:
        print("Calcul de la prime annuelle...")
        df_employee_prime = df_employee_prime.withColumn(
            "prime_annuelle", 
            round(col("salaire_brut") * 0.05, 2)
        )
    
    # Sélectionner les colonnes nécessaires (en minuscules maintenant)
    df_employee_selected = df_employee_prime.select("id_salarie", "salaire_brut", "moyen_deplacement", "prime_annuelle")
    print("Colonnes sélectionnées de employee_prime:")
    df_employee_selected.show(5, truncate=False)
    
except Exception as e:
    print(f"Erreur lors de la lecture de la table Delta employee_prime: {e}")
    df_employee_prime = None
    df_employee_selected = None

# ANALYSE PRINCIPALE
print("\n" + "="*70)
print("ANALYSE: EMPLOYÉS AVEC ≥15 ACTIVITÉS SPORTIVES")
print("="*70)

if donnees_rh_df is not None and donnees_sportive_df is not None and df_delta is not None:
    try:
        # 1. Compter les activités Excel (Pratique_sport non vide)
        print("1. Calcul des activités Excel...")
        activites_excel = donnees_sportive_df.groupBy("id_salarie") \
            .agg(count(when(col("pratique_sport") != "", 1)).alias("nb_activites_excel"))
        
        activites_excel.show(5)

        # 2. Compter les activités Delta
        print("2. Calcul des activités Delta...")
        # Trouver la colonne ID dans Delta
        id_col_delta = None
        for col_name in df_delta.columns:
            if 'id' in col_name.lower() or 'salari' in col_name.lower():
                id_col_delta = col_name
                break
        
        if not id_col_delta:
            id_col_delta = df_delta.columns[0]  # Prendre la première colonne
            print(f"Utilisation de la colonne '{id_col_delta}' comme ID Delta")
        
        activites_delta = df_delta.groupBy(id_col_delta) \
            .agg(count("*").alias("nb_activites_delta"))
        
        activites_delta.show(5)

        # 3. Jointure des activités Excel + Delta
        print("3. Jointure des activités...")
        # Renommer la colonne ID de Delta pour éviter l'ambiguïté
        activites_delta_renamed = activites_delta.withColumnRenamed(id_col_delta, "id_salarie_delta")
        
        activites_totales = activites_excel.join(
            activites_delta_renamed,
            activites_excel["id_salarie"] == activites_delta_renamed["id_salarie_delta"],
            "full_outer"
        ).fillna(0)

        # Calcul du total
        activites_totales = activites_totales.withColumn(
            "nb_activites_total", 
            col("nb_activites_excel") + col("nb_activites_delta")
        )

        # 4. Filtrer ≥15 activités
        print("4. Filtrage ≥15 activités...")
        employes_15_activites = activites_totales.filter(
            col("nb_activites_total") >= 15
        ).orderBy(col("nb_activites_total").desc())

        print(f"Employés avec ≥15 activités: {employes_15_activites.count()}")
        employes_15_activites.show()

        # 5. Jointure avec données RH pour les détails
        print("5. Jointure avec données RH...")
        # Utiliser un alias pour éviter l'ambiguïté
        employes_details = employes_15_activites.join(
            donnees_rh_df.alias("rh"),
            employes_15_activites["id_salarie"] == col("rh.id_salarie"),
            "inner"
        ).select(
            col("rh.id_salarie"),
            col("rh.nom"),
            col("rh.prenom"),
            col("rh.moyen_deplacement"),
            col("rh.salaire_brut"),  # Ajout du salaire brut
            col("nb_activites_excel"),
            col("nb_activites_delta"),
            col("nb_activites_total")
        ).orderBy(col("nb_activites_total").desc())

        print("Détails des employés très actifs:")
        employes_details.show(truncate=False)

        # 6. Filtrer les moyens de déplacement écologiques
        print("6. Filtrage déplacements écologiques...")
        employes_ecologiques = donnees_rh_df.filter(
            col("moyen_deplacement").isin(["Marche/running", "Vélo/Trottinette/Autres", 
                                         "Marche_running", "Velo_Trottinette_Autres"])
        )

        print(f"Employés écologiques : {employes_ecologiques.count()}")
        
        if employes_ecologiques.count() > 0:
            employes_ecologiques.show(truncate=False, n=20)
        else:
            print("Aucun employé écologique avec ≥15 activités")

        # 7. Intersection des employés très actifs et écologiques
        print("7. Intersection des employés très actifs et écologiques...")
        employes_intersection = employes_details.join(
            employes_ecologiques,
            employes_details["id_salarie"] == employes_ecologiques["id_salarie"],
            "inner"
        )

        print(f"Employés très actifs et écologiques : {employes_intersection.count()}")
        employes_intersection.show(truncate=False)
        
        # 8. CALCUL DU BUDGET DES PRIMES
        print("\n" + "="*70)
        print("CALCUL DU BUDGET DES PRIMES ÉCOLOGIQUES")
        print("="*70)
        
        # Calculer la prime pour chaque employé écologique (5% du salaire annuel)
        employes_ecologiques_prime = employes_ecologiques.withColumn(
            "prime_annuelle", 
            round(col("salaire_brut") * 0.05, 2)
        )
        
        # Afficher le détail des primes
        print("Détail des primes par employé écologique:")
        employes_ecologiques_prime.select(
            "id_salarie", "nom", "prenom", "moyen_deplacement", 
            "salaire_brut", "prime_annuelle"
        ).orderBy(col("prime_annuelle").desc()).show(truncate=False)
        
        # Calcul du budget total
        budget_total = employes_ecologiques_prime.agg(
            spark_sum("prime_annuelle").alias("budget_total_primes"),
            count("*").alias("nombre_employes_ecologiques"),
            avg("prime_annuelle").alias("prime_moyenne")
        )
        
        print("BUDGET TOTAL DES PRIMES ÉCOLOGIQUES:")
        budget_total.show(truncate=False)
        
        # Détail du budget
        budget_result = budget_total.collect()[0]
        print(f"\n📊 RÉSUMÉ DU BUDGET:")
        print(f"• Nombre d'employés écologiques: {budget_result['nombre_employes_ecologiques']}")
        print(f"• Prime moyenne par employé: {budget_result['prime_moyenne']:.2f} €")
        print(f"• BUDGET TOTAL À PRÉVOIR: {budget_result['budget_total_primes']:.2f} €")
        print(f"• Pourcentage du salaire: 5% du salaire annuel brut")
        
    except Exception as e:
        print(f"Erreur lors de l'analyse principale : {e}")
        import traceback
        traceback.print_exc()
else:
    print("Données manquantes pour l'analyse")

# ANALYSE AVEC LA TABLE EMPLOYEE_PRIME
if df_employee_selected is not None:
    print("\n" + "="*70)
    print("ANALYSE AVEC LA TABLE EMPLOYEE_PRIME")
    print("="*70)
    
    try:
        # Afficher les statistiques de base
        print("Statistiques de base de employee_prime:")
        df_employee_selected.describe().show()
        
        # Calculer le salaire moyen par moyen de déplacement
        print("Salaire moyen par moyen de déplacement:")
        salaire_par_deplacement = df_employee_selected.groupBy("moyen_deplacement") \
            .agg(
                round(avg("salaire_brut"), 2).alias("Salaire_moyen"),
                count("*").alias("Nombre_employes")
            ) \
            .orderBy(col("Salaire_moyen").desc())
        
        salaire_par_deplacement.show(truncate=False)
        
        # Compter le nombre d'employés par moyen de déplacement
        print("Répartition des employés par moyen de déplacement:")
        employes_par_deplacement = df_employee_selected.groupBy("moyen_deplacement") \
            .count() \
            .orderBy(col("count").desc())
            
        employes_par_deplacement.show(truncate=False)

        # Afficher la prime moyenne par moyen de déplacement
        print("Prime moyenne par moyen de déplacement:")
        prime_par_deplacement = df_employee_selected.groupBy("moyen_deplacement") \
            .agg(
                round(avg("prime_annuelle"), 2).alias("Prime_moyenne"),
                count("*").alias("Nombre_employes")
            ) \
            .orderBy(col("Prime_moyenne").desc())
        
        prime_par_deplacement.show(truncate=False)
        
    except Exception as e:
        print(f"Erreur lors de l'analyse de employee_prime: {e}")
        import traceback
        traceback.print_exc()

# Stop Spark
print("\n" + "="*70)
print("ANALYSE TERMINÉE")
print("="*70)

# Stop Spark
spark.stop()