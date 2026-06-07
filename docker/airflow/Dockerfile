FROM apache/airflow:2.9.3-python3.10

USER root

ARG SPARK_VERSION=3.5.1
ARG ICEBERG_VERSION=1.5.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.3

# Java + tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

# Apache Spark (provides spark-submit for the cluster client)
RUN set -eux; \
    curl -fSL -o /tmp/spark.tgz \
      https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz; \
    mkdir -p ${SPARK_HOME}; \
    tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1; \
    rm /tmp/spark.tgz

# Same extra jars as the Spark cluster image (jar parity for client mode)
RUN set -eux; \
    cd ${SPARK_HOME}/jars; \
    curl -fSL -o iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fSL -o hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fSL -o aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar; \
    curl -fSL -o postgresql-${POSTGRES_JDBC_VERSION}.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
RUN chown -R airflow: ${SPARK_HOME}

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
