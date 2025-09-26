FROM openjdk:11-slim

# Installer Python et outils nécessaires
RUN apt-get update && apt-get install -y python3 python3-pip curl && apt-get clean

# Déclaration des variables d'environnement
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install Python + pip + pandas
RUN apt-get update && apt-get install -y python3 python3-pip curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    pip3 install pandas openpyxl

# Téléchargement et installation de Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Créer répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires
COPY spark_powerbi.py .
COPY requirements.txt .
COPY Donnees_RH.xlsx .
COPY Donnees_Sportive.xlsx .

# Install PySpark and delta-spark
RUN pip3 install --no-cache-dir pyspark==3.5.0 delta-spark==3.2.0

# Commande de lancement Spark pour Power BI
CMD ["spark-submit", "--packages", "io.delta:delta-spark_2.12:3.2.0", "spark_powerbi.py"]