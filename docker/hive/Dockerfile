FROM apache/hive:4.0.0

USER root

ARG HADOOP_AWS_VERSION=3.3.6
ARG AWS_SDK_VERSION=1.12.367
ARG POSTGRES_JDBC_VERSION=42.7.3

# The apache/hive base image (Debian 11) ships neither curl nor wget, so install
# curl (+ CA certs) to download the jars with retry/resume. apt is told to retry
# too, to survive flaky networks.
RUN printf 'Acquire::Retries "8";\n' > /etc/apt/apt.conf.d/80-retries && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# S3A + Postgres JDBC support for the metastore
RUN set -eux; \
    cd /opt/hive/lib; \
    for url in \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" ; do \
        curl --fail --location --retry 8 --retry-delay 5 \
             --connect-timeout 30 --continue-at - -O "$url"; \
    done; \
    chmod 644 hadoop-aws-${HADOOP_AWS_VERSION}.jar \
              aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
              postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY conf/metastore-site.xml /opt/hive/conf/metastore-site.xml
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

USER hive
ENTRYPOINT ["/opt/entrypoint.sh"]
