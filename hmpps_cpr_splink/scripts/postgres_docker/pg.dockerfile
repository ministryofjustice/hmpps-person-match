FROM postgres:16.1

COPY dummy_data.sql /docker-entrypoint-initdb.d/01_dummy_data.sql
COPY dummy_tfs.sql /docker-entrypoint-initdb.d/02_dummy_tfs.sql
