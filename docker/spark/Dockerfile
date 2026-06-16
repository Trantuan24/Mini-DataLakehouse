FROM apache/spark:3.5.1

USER root

ARG ICEBERG_VERSION=1.5.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.3

# CRITICAL: align the executor JVM with the Spark driver. The driver runs inside
# the airflow image (Java 17); this base image ships Java 11. Spark requires the
# driver and ALL executors to run the SAME major Java version — a mismatch makes
# task-result (Kryo) deserialization fail with java.io.EOFException. Install
# Java 17 here and point JAVA_HOME at it.
RUN printf 'Acquire::Retries "8";\n' > /etc/apt/apt.conf.d/80-retries && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# NOTE on Python parity: PySpark requires the executor Python MINOR version to
# match the driver. This base ships Python 3.8, so the airflow (driver) image is
# pinned to the matching apache/airflow:2.9.3-python3.8 tag instead of pulling a
# newer Python here (avoids a flaky third-party PPA on slow networks).

# Iceberg + S3A + Postgres JDBC jars on the Spark classpath.
# Downloaded with curl using retry + resume (--continue-at -) so a dropped or
# slow connection retries instead of failing the whole build. The base
# apache/spark image already ships curl.
RUN set -eux; \
    cd /opt/spark/jars; \
    for url in \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" ; do \
        curl --fail --location --retry 8 --retry-delay 5 \
             --connect-timeout 30 --continue-at - -O "$url"; \
    done; \
    chmod 644 iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
              hadoop-aws-${HADOOP_AWS_VERSION}.jar \
              aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
              postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
