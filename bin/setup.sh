#!/bin/bash

# mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init

docker-compose up -d