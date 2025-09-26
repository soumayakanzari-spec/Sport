import os
import threading
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, round, when, lit, udf
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType, DecimalType
from delta import configure_spark_with_delta_pip

# Paths and table settings
DELTA_PATH = "/data/delta_sport_activities"
EMPLOYEE_PRIME_PATH = "/data/delta_employee_prime" 
DB_NAME = "demo_db"
TABLE_NAME = "sport_activities_delta"
EMPLOYEE_TABLE_NAME = "employee_prime"

# Créer les répertoires Delta s'ils n'existent pas
os.makedirs("/data/delta_sport_activities", exist_ok=True)
os.makedirs("/data/delta_employee_prime", exist_ok=True)
os.makedirs("/tmp/checkpoints/sport_activities", exist_ok=True)
os.makedirs("/tmp/checkpoints/employee_prime", exist_ok=True)

# Python environment inside Spark workers
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'

# Build SparkSession
builder = SparkSession.builder \
    .appName("KafkaSportActivitiesProcessor") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")


spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Add this before writing to Delta
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

spark.sparkContext.setLogLevel("WARN")

# Schema for Debezium "after" payload - Sport Activities
payload_schema = StructType() \
    .add("id_salarie", StringType()) \
    .add("start_date", TimestampType()) \
    .add("activity_type", StringType()) \
    .add("distance_m", FloatType()) \
    .add("duration_sec", IntegerType()) \
    .add("comment", StringType())

# Full Debezium schema
debezium_schema = StructType().add("payload", StructType().add("after", payload_schema))

# Schema for Employee Prime data - CORRIGÉ pour correspondre au schéma réel
employee_schema = StructType() \
    .add("id_salarie", StringType()) \
    .add("salaire_brut", StringType()) \
    .add("moyen_deplacement", StringType()) \
    .add("nom", StringType()) \
    .add("prenom", StringType()) \
    .add("date_embauche", TimestampType()) \
    .add("type_contrat", StringType()) \
    .add("bu", StringType())

debezium_employee_schema = StructType().add("payload", StructType().add("after", employee_schema))


# Read Kafka stream for sport activities
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "postgres_server.public.sport_activities") \
    .option("startingOffsets", "earliest") \
    .load()

# Read Kafka stream for employee prime
df_kafka_employee = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "postgres_server.public.employee_prime") \
    .option("startingOffsets", "earliest") \
    .load()

# Debug: Afficher les données brutes pour voir le format des messages
print("Debug: Affichage des messages bruts Kafka pour employee_prime")

# Extract "after" data from sport activities
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), debezium_schema).alias("data")) \
    .select("data.payload.after.*")

# Extract "after" data from employee prime
df_employee_parsed = df_kafka_employee.select(
    from_json(col("value").cast("string"), debezium_employee_schema).alias("data"),
    col("value").cast("string").alias("raw_value")
).select(
    "data.payload.after.*",
    "raw_value"
)

# Définir une UDF pour convertir les salaires
def convert_salaire(salaire_str):
    try:
        # Essayer de convertir en float
        return float(salaire_str)
    except:
        # Si échec, retourner une valeur par défaut
        return 30000.0

convert_salaire_udf = udf(convert_salaire, FloatType())

# Ajouter une colonne avec le salaire converti pour le calcul
# Extract "after" data from employee prime
df_employee_parsed = df_kafka_employee.select(
    from_json(col("value").cast("string"), debezium_employee_schema).alias("data"),
    col("value").cast("string").alias("raw_value")
).select(
    "data.payload.after.*",
    "raw_value"
)

# Définir une UDF pour convertir les salaires
def convert_salaire(salaire_str):
    try:
        # Essayer de convertir en float
        return float(salaire_str)
    except:
        # Si échec, retourner une valeur par défaut
        return 30000.0

convert_salaire_udf = udf(convert_salaire, FloatType())

# Ajouter une colonne avec le salaire converti pour le calcul
df_employee_parsed = df_employee_parsed.withColumn(
    "salaire_brut_float", 
    convert_salaire_udf(col("salaire_brut"))
)

# Ajouter un stream de débogage pour voir les valeurs de moyen_deplacement
debug_query = df_employee_parsed.select("moyen_deplacement") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Attendre un peu pour voir les données de débogage
time.sleep(10)

# Arrêter le stream de débogage
debug_query.stop()

# Définir les moyens de déplacement écologiques basés sur ce que vous avez vu dans le débogage
moyens_ecologiques = ["Marche/running", "Vélo/Trottinette/Autres", "Marche_running", "Velo_Trottinette_Autres"]

# Ajouter le calcul de la prime annuelle SEULEMENT pour les moyens de déplacement écologiques
df_employee_parsed = df_employee_parsed.withColumn(
    "prime_annuelle", 
    when(col("moyen_deplacement").isin(moyens_ecologiques), round(col("salaire_brut_float") * 0.05, 2))
    .otherwise(lit(0.0))
)


# Ajouter une colonne de timestamp pour le débogage
df_employee_parsed = df_employee_parsed.withColumn("processing_timestamp", current_timestamp())

# Save streaming data to Delta for sport activities
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", DELTA_PATH) \
    .option("checkpointLocation", "/tmp/checkpoints/sport_activities") \
    .option("mergeSchema", "true") \
    .start()

# Fonction pour traiter les lots d'employee avec gestion d'erreurs
def process_employee_batch(df, epoch_id, path):
    try:
        # Afficher les données brutes pour le débogage
        print(f"=== Données brutes reçues (epoch {epoch_id}) ===")
        df.select("raw_value").show(truncate=False, n=5)
        
        # Compter les lignes avec des valeurs nulles
        null_counts = {column_name: df.filter(col(column_name).isNull()).count() for column_name in df.columns if column_name not in ['raw_value', 'processing_timestamp']}
        print("=== Comptage des valeurs nulles ===")
        for col_name, count in null_counts.items():
            print(f"{col_name}: {count}")
        
        # Remplir les valeurs manquantes avec des valeurs par défaut
        df_filled = df.fillna({
            'salaire_brut': '30000',
            'moyen_deplacement': 'Non spécifié',
            'nom': 'Inconnu',
            'prenom': 'Inconnu',
            'type_contrat': 'CDI',
            'bu': 'Non spécifié'
        })
        
        # Recalculer la prime avec les valeurs corrigées (seulement pour moyens écologiques)
        df_filled = df_filled.withColumn(
            "salaire_brut_float", 
            convert_salaire_udf(col("salaire_brut"))
        ).withColumn(
            "prime_annuelle", 
            when(col("moyen_deplacement").isin(moyens_ecologiques), round(col("salaire_brut_float") * 0.05, 2))
            .otherwise(lit(0.0))
        )
        
        # Filtrer seulement les lignes avec un ID salarie valide
        valid_df = df_filled.filter(
            col("id_salarie").isNotNull()
        ).drop("raw_value", "processing_timestamp", "salaire_brut_float")  # Supprimer la colonne temporaire
        
        # Écrire les données valides
        if valid_df.count() > 0:
            valid_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(path)
            
            print(f"✅ {valid_df.count()} lignes valides écrites dans Delta")
            
            # Afficher un aperçu des données écrites
            print("Aperçu des données écrites:")
            valid_df.show(5, truncate=False)
            
            # Afficher le nombre d'employés éligibles à la prime
            employes_ecologiques = valid_df.filter(col("prime_annuelle") > 0).count()
            print(f"📊 {employes_ecologiques} employés éligibles à la prime écologique")
        else:
            print("⚠️ Aucune donnée valide à écrire")
        
    except Exception as e:
        print(f"❌ Erreur lors du traitement du lot: {e}")
        import traceback
        traceback.print_exc()

# Save streaming data to Delta for employee prime - AVEC GESTION D'ERREURS
query_employee = df_employee_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", EMPLOYEE_PRIME_PATH) \
    .option("checkpointLocation", "/tmp/checkpoints/employee_prime") \
    .option("mergeSchema", "true") \
    .foreachBatch(lambda df, epoch_id: process_employee_batch(df, epoch_id, EMPLOYEE_PRIME_PATH)) \
    .start()

# Also register as managed Delta tables in Spark catalog
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
spark.sql(f"USE {DB_NAME}")

# Attendre que les données commencent à être écrites avant de créer les tables
time.sleep(30)  # Attendre 30 secondes

# Fonction pour créer les tables seulement si elles n'existent pas déjà
def create_table_if_not_exists(table_name, path):
    try:
        # Vérifier si le chemin Delta existe et contient des données
        df_test = spark.read.format("delta").load(path)
        if df_test.count() > 0:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name} 
                USING DELTA 
                LOCATION '{path}'
            """)
            print(f"✅ Table {table_name} créée avec succès")
        else:
            print(f"⚠️ Le chemin {path} existe mais ne contient pas de données")
    except Exception as e:
        print(f"❌ Impossible de créer la table {table_name}: {e}")

# Créer les tables seulement si elles n'existent pas déjà
create_table_if_not_exists(TABLE_NAME, DELTA_PATH)
create_table_if_not_exists(EMPLOYEE_TABLE_NAME, EMPLOYEE_PRIME_PATH)

print("Spark version:", spark.version)

# Function to read and process employee_prime table
def read_employee_data():
    while True:
        try:
            # Lire directement depuis le chemin Delta au lieu de la table
            df_employee = spark.read.format("delta").load(EMPLOYEE_PRIME_PATH)
            
            print(f"\nDonnées de la table employee_prime (lecture directe du chemin Delta):")
            df_employee.show(10)
            
            # Vérifier si la table contient des données
            if df_employee.count() > 0:
                # Optional: Create a temporary view for SQL queries
                df_employee.createOrReplaceTempView("employee_filtered")
                
                # Example SQL query - Prime moyenne par moyen de déplacement
                result = spark.sql("""
                    SELECT moyen_deplacement, 
                           AVG(prime_annuelle) as Prime_moyenne,
                           COUNT(*) as Nombre_employes,
                           SUM(prime_annuelle) as Budget_total
                    FROM employee_filtered
                    GROUP BY moyen_deplacement
                    ORDER BY Prime_moyenne DESC
                """)
                
                print("Prime moyenne par moyen de déplacement:")
                result.show()
                
                # Calcul du budget total des primes écologiques
                budget_total = spark.sql("""
                    SELECT SUM(prime_annuelle) as Budget_total_primes
                    FROM employee_filtered
                    WHERE prime_annuelle > 0
                """)
                
                print("Budget total des primes écologiques:")
                budget_total.show()
            else:
                print("La table employee_prime est vide")
            
        except Exception as e:
            print(f"Erreur lors de la lecture du chemin Delta {EMPLOYEE_PRIME_PATH}: {e}")
            print("Le chemin Delta n'existe peut-être pas encore. Réessai dans 30 secondes...")
        
        # Wait before next read
        time.sleep(30)

# Start a thread to periodically read employee data
employee_thread = threading.Thread(target=read_employee_data, daemon=True)
employee_thread.start()

print("Démarrage du traitement Kafka pour sport_activities et employee_prime...")

# Wait for both streaming queries
spark.streams.awaitAnyTermination()