#!/usr/bin/env sh

sysbench \
    --db-driver=mysql \
    --mysql-host=127.0.0.1 \
    --mysql-port=4000 \
    --mysql-user=root \
    --mysql-db=test \
    --threads=1 \
    --table_size=10000 \
    --tables=10 \
    oltp_point_select \
    prepare && \
sysbench \
    --db-driver=mysql \
    --mysql-host=127.0.0.1 \
    --mysql-port=4000 \
    --mysql-user=root \
    --mysql-db=test \
    --threads=4 \
    --table_size=10000 \
    --tables=10 \
    --time=999999 \
    oltp_point_select \
    run
