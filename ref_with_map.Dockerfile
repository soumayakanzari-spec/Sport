FROM openjdk:11-slim

# Installer les dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    wget \
    python3 \
    python3-pip \
    python3-dev \
    libxml2-dev \
    libxslt-dev \
    libgomp1 \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Configurer Python 3 comme par défaut
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Mettre à jour pip et installer les dépendances Python
RUN python3 -m pip install --upgrade pip setuptools

# INSTALLER PYARROW OBLIGATOIREMENT
RUN pip3 install --no-cache-dir pyarrow>=1.0.0

# Télécharger et installer Spark
ENV SPARK_VERSION=3.4.3
ENV HADOOP_VERSION=3
RUN wget -O spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark.tgz

# Configurer les variables d'environnement Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copier les requirements et installer les dépendances Python
COPY requirements2.txt .
RUN pip3 install --no-cache-dir --upgrade -r requirements2.txt

# Installer les packages JAR pour Delta Lake
RUN curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    mv delta-core_2.12-2.4.0.jar /opt/spark/jars/

# Configurer la variable d'environnement Google Maps
ENV GOOGLE_MAPS_API_KEY=${GOOGLE_MAPS_API_KEY}

# Créer le répertoire de travail
WORKDIR /app

# Copier le script Python avec Google Maps
COPY spark_ref_with_maps.py .

# Vérifier l'installation des packages
RUN python3 -c "import pyarrow; print('PyArrow version:', pyarrow.__version__)"
RUN python3 -c "import pandas; print('pandas version:', pandas.__version__)"
RUN python3 -c "import googlemaps; print('googlemaps installed')"

# Point d'entrée
CMD ["python3", "spark_ref_with_maps.py"]