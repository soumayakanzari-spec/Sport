from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("Debezium Kafka Consumer with Spark") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Topic Debezium (à adapter selon ton cas)
kafka_topic = "postgres_server.public.sport_activities"  # Remplace avec le nom de ton topic
kafka_bootstrap_servers = "redpanda:9092"     # Redpanda ou localhost selon ton contexte

# Lecture en streaming depuis Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Les colonnes 'key' et 'value' sont en binaire => cast en string
df_string = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Affichage brut (json string dans value)
query = df_string.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
