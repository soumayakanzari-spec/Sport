FROM openjdk:11-slim

# Installer les dépendances système
RUN apt-get update && apt-get install -y python3 python3-pip curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Déclaration des variables d'environnement
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Téléchargement et installation de Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Installer le driver PostgreSQL JDBC
RUN curl -L -o /opt/spark/jars/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Installer les packages Python
RUN pip3 install --no-cache-dir pandas openpyxl pyspark==3.5.0 delta-spark==3.2.0

# Créer répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires
COPY spark_ref.py .
COPY requirements.txt .
COPY Donnees_RH.xlsx .  
COPY Donnees_Sportive.xlsx .

# Commande de lancement Spark
CMD ["spark-submit", "--packages", "io.delta:delta-spark_2.12:3.2.0", "spark_ref.py"]