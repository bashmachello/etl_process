FROM apache/airflow:3.3.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pyspark==4.2.0 \
    clickhouse-connect==1.8.0

ARG TARGETARCH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}

RUN mkdir -p /home/airflow/jars \
    && curl -fL -o /home/airflow/jars/postgresql-42.7.4.jar \
       https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar