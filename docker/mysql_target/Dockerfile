# AstraTune MySQL target：对齐 BIRD-CRITIC evaluation MySQL 初始化思路
ARG MYSQL_IMAGE=mysql:8.4.0
FROM ${MYSQL_IMAGE}

ENV MYSQL_ROOT_PASSWORD=123123

COPY init-databases_mysql.sh /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/init-databases_mysql.sh
