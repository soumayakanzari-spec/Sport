import os
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, when, sum as spark_sum, avg, round, udf, lit
from pyspark.sql.types import FloatType, DecimalType
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import format_number, regexp_replace
from pyspark.sql.functions import round, col, regexp_replace

# Path to existing Delta tables
DELTA_PATH = "/data/delta_sport_activities"
EMPLOYEE_PRIME_PATH = "/data/delta_employee_prime"

# Python environment inside Spark workers
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'

# Build SparkSession with Delta
builder = SparkSession.builder \
    .appName("PowerBIExporter") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Définir une UDF pour convertir les salaires
def convert_salaire(salaire_str):
    try:
        return float(salaire_str)
    except:
        return 30000.0

convert_salaire_udf = udf(convert_salaire, FloatType())

# Fonction pour exporter les données en temps réel
def export_realtime_data():
    export_dir = "/data/powerbi_export"
    os.makedirs(export_dir, exist_ok=True)

    moyens_ecologiques = ["Marche/running", "Vélo/Trottinette/Autres", "Marche_running", "Velo_Trottinette_Autres"]

    while True:
        try:
            print("\n" + "="*70)
            print("LECTURE DES DONNÉES DELTA POUR EXPORT POWER BI")
            print("="*70)

            # Lire la table Delta des activités sportives
            try:
                df_activities = spark.read.format("delta").load(DELTA_PATH)
                activities_count = df_activities.count()
                print(f"Nombre d'activités dans Delta: {activities_count}")
            except Exception as e:
                print(f"Erreur lors de la lecture des activités: {e}")
                df_activities = None

            # Lire la table Delta des primes employés
            try:
                df_employee = spark.read.format("delta").load(EMPLOYEE_PRIME_PATH)

                if "prime_annuelle" not in df_employee.columns:
                    df_employee = df_employee.withColumn(
                        "salaire_brut_float",
                        convert_salaire_udf(col("salaire_brut"))
                    )
                    df_employee = df_employee.withColumn(
                        "prime_annuelle",
                        when(col("moyen_deplacement").isin(moyens_ecologiques),
                             round(col("salaire_brut_float") * 0.05, 2))
                        .otherwise(lit(0.0))
                    )

                df_primes = df_employee.select(
                    col("id_salarie").alias("ID_employee"),
                    "moyen_deplacement",
                    col("prime_annuelle").cast(DecimalType(10, 2)).alias("prime_annuelle"),
                    convert_salaire_udf(col("salaire_brut")).cast(DecimalType(10, 2)).alias("salaire_brut")
                )

                print(f"Nombre d'employés dans Delta (avant déduplication): {df_primes.count()}")

            except Exception as e:
                print(f"Erreur lors de la lecture des primes: {e}")
                df_primes = None

            # Fonction d’export avec pandas déduplication
            def export_single_file(df, filename):
                if df is not None and df.count() > 0:
                    try:
                        temp_path = f"{export_dir}/temp_{filename}"
                        df.coalesce(1).write \
                            .mode("overwrite") \
                            .option("header", "true") \
                            .option("delimiter", ";") \
                            .option("decimal", ".") \
                            .csv(temp_path)

                        import glob, shutil
                        csv_files = glob.glob(f"{temp_path}/*.csv")
                        if not csv_files:
                            print(f"❌ Aucun fichier généré pour {filename}")
                            return False

                        final_path = f"{export_dir}/{filename}"
                        shutil.move(csv_files[0], final_path)
                        shutil.rmtree(temp_path)

                        # --- Post-traitement pandas : garder la dernière version ---
                        try:
                            pdf = pd.read_csv(final_path, sep=';')

                            if 'ID_employee' in pdf.columns:
                                id_col = 'ID_employee'
                            elif 'id_salarie' in pdf.columns:
                                id_col = 'id_salarie'
                            else:
                                print("⚠️ Pas de colonne ID trouvée, pas de déduplication.")
                                return True

                            # garder la dernière occurrence
                            pdf_dedup = pdf.drop_duplicates(subset=[id_col], keep='last')

                            pdf_dedup.to_csv(final_path, sep=';', index=False, float_format="%.2f")

                            print(f"✅ Export (dédupliqué) réussi: {final_path}")
                            print(f"📊 Lignes exportées après dédup: {len(pdf_dedup)}")
                            print("Aperçu:")
                            print(pdf_dedup.head().to_string(index=False))

                        except Exception as e_pd:
                            print(f"⚠️ Erreur pandas lors du dédup: {e_pd}")
                            print(f"✅ Export brut conservé: {final_path}")

                        return True

                    except Exception as e:
                        print(f"❌ Erreur export {filename}: {e}")
                        return False
                else:
                    print(f"⚠️ Aucune donnée à exporter pour {filename}")
                    return False
                

            def export_single_fileWithDuplication(df, filename):
                if df is not None and df.count() > 0:
                    try:
                        temp_path = f"{export_dir}/temp_{filename}"
                        df.coalesce(1).write \
                            .mode("overwrite") \
                            .option("header", "true") \
                            .option("delimiter", ";") \
                            .option("decimal", ".") \
                            .csv(temp_path)

                        import glob, shutil
                        csv_files = glob.glob(f"{temp_path}/*.csv")
                        if not csv_files:
                            print(f"❌ Aucun fichier généré pour {filename}")
                            return False

                        final_path = f"{export_dir}/{filename}"
                        shutil.move(csv_files[0], final_path)
                        shutil.rmtree(temp_path)

                        # --- Post-traitement pandas : garder la dernière version ---
                        try:
                            pdf = pd.read_csv(final_path, sep=';')

                            if 'ID_employee' in pdf.columns:
                                id_col = 'ID_employee'
                            elif 'id_salarie' in pdf.columns:
                                id_col = 'id_salarie'
                            else:
                                print("⚠️ Pas de colonne ID trouvée, pas de déduplication.")
                                return True

                            # garder la dernière occurrence
                            #pdf_dedup = pdf.drop_duplicates(subset=[id_col], keep='last')

                            pdf.to_csv(final_path, sep=';', index=False, float_format="%.2f")

                            print(f"✅ Export (with dédupliqué) réussi: {final_path}")
                            print(f"📊 Lignes exportées après dédup: {len(pdf)}")
                            print("Aperçu:")
                            print(pdf.head().to_string(index=False))

                        except Exception as e_pd:
                            print(f"⚠️ Erreur pandas lors du dédup: {e_pd}")
                            print(f"✅ Export brut conservé: {final_path}")

                        return True

                    except Exception as e:
                        print(f"❌ Erreur export {filename}: {e}")
                        return False
                else:
                    print(f"⚠️ Aucune donnée à exporter pour {filename}")
                    return False



            df_primes_export = df_primes.withColumn(
                "prime_annuelle",
                regexp_replace(round(col("prime_annuelle"), 2).cast("string"), r"\.", ",")
            ).withColumn(
                "salaire_brut",
                regexp_replace(round(col("salaire_brut"), 2).cast("string"), r"\.", ",")
            )


            # Exporter les fichiers
            export_single_fileWithDuplication(df_activities, "activites_sportives.csv")
            print("\n" + "-"*50)
            export_single_file( df_primes_export, "primes_employes.csv")




            # Exporter des statistiques supplémentaires
            if df_primes is not None:
                stats_primes = df_primes.groupBy("moyen_deplacement") \
                    .agg(
                        count("*").alias("Nombre_employes"),
                        round(avg("prime_annuelle"), 2).alias("Prime_moyenne"),
                        round(spark_sum("prime_annuelle"), 2).alias("Budget_total")
                    ) \
                    .orderBy(col("Budget_total").desc())
                export_single_file(stats_primes, "statistiques_primes.csv")

            print("\n" + "="*70)
            print("✅ EXPORT POWER BI TERMINÉ!")
            print("="*70)

        except Exception as e:
            print(f"❌ Erreur lors de l'export: {e}")
            import traceback
            traceback.print_exc()

        print("\n⏳ Attente de 30 secondes avant le prochain export...")
        time.sleep(30)

# Démarrer l'export en temps réel
export_realtime_data()

# Stop Spark
spark.stop()
