# BIRD-CRITIC evaluation Dockerfile.postgresql equivalent; override base when Hub fails.
ARG POSTGRES_IMAGE=postgres:14.12
FROM ${POSTGRES_IMAGE}

ENV POSTGRES_USER=root
ENV POSTGRES_PASSWORD=123123

RUN apt-get update \
    && apt-get install -y postgis postgresql-14-postgis-3 \
    && rm -rf /var/lib/apt/lists/*

COPY init-databases_postgresql.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/init-databases_postgresql.sh
