FROM apache/superset:3.1.3

USER root
# Trino SQLAlchemy dialect + Postgres driver for metadata DB
RUN pip install --no-cache-dir \
      "trino[sqlalchemy]==0.328.0" \
      sqlalchemy-trino==0.5.0 \
      psycopg2-binary==2.9.9

COPY bootstrap.sh /app/docker/bootstrap.sh
COPY create_trino_db.py /app/docker/create_trino_db.py
RUN chmod +x /app/docker/bootstrap.sh

USER superset
ENTRYPOINT ["/app/docker/bootstrap.sh"]
