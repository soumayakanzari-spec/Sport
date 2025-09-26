FROM openjdk:11-slim

# Installer Python et outils nécessaires
RUN apt-get update && apt-get install -y python3 python3-pip curl && apt-get clean


#Déclaration des variables d’environnement:
#Définit la version de Spark et de Hadoop
#Configure le chemin d'installation de Spark (/opt/spark)
#Ajoute Spark au PATH pour qu’il soit exécutable directement
# Installer Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH



# Install Python + pip
RUN apt-get update && apt-get install -y python3 python3-pip curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
#Téléchargement et installation de Spark:
#Télécharge l’archive Spark 3.5.0 compatible avec Hadoop 3
#La décompresse dans /opt/
#La renomme en /opt/spark pour simplifier le chemin
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Créer répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires
COPY spark_traitement.py .
COPY spark_ref.py .
COPY requirements.txt .

# Install PySpark and delta-spark
RUN pip3 install --no-cache-dir pyspark==3.5.0 delta-spark==3.2.0

#Installation des packages Python
# Installer les dépendances Python
#RUN pip3 install --no-cache-dir -r requirements.txt

# Commande de lancement Spark:
    #Lire depuis Kafka
    #Utiliser le package spark-sql-kafka-0-10 pour lire les données Kafka
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0", "spark_traitement.py"]