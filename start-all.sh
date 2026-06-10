#!/bin/bash

set -e

echo "🚧 Creating networks (if not exist)..."

docker network create hadoop_net || true
docker network create spark_net || true 
docker network create airflow_net || true
docker network create mongo_net || true
docker network create nessie_net || true
docker network create data_eng_net || true

echo "✅ Networks ready"

ENV_FILE=".env.global"

echo "🚀 Starting MySQL (nessie backend)..."
docker compose -p nessie \
  --project-directory . \
  -f ./docker/nessie/docker-compose.yaml \
  --env-file $ENV_FILE \
  up -d mysql-backend

echo "⏳ Waiting MySQL to be healthy..."

# tunggu sampai container healthy
until [ "$(docker inspect --format='{{.State.Health.Status}}' mysql-backend 2>/dev/null)" == "healthy" ]; do
  echo "⌛ waiting mysql..."
  sleep 5
done

echo "✅ MySQL is healthy"

echo "🚀 Starting Nessie..."
docker compose -p nessie \
  --project-directory . \
  -f ./docker/nessie/docker-compose.yaml \
  --env-file $ENV_FILE \
  up -d nessie-rest-catalog

echo "🚀 Starting Hadoop..."
docker compose -p hadoop \
  --project-directory . \
  -f ./docker/hadoop/docker-compose.yaml \
  --env-file $ENV_FILE \
  up -d

echo "⏳ Waiting for Hadoop to be ready..."
sleep 10

docker exec -it namenode bash -c "
  hdfs dfs -mkdir -p /warehouse_dev && 
  hdfs dfs -mkdir -p /warehouse
"

echo "🚀 Starting Mongo..."
docker compose -p mongo \
  --project-directory . \
  -f ./docker/mongo/docker-compose.yaml \
  --env-file $ENV_FILE \
  up -d

echo "🚀 Starting Spark..."
docker compose -p spark \
  --project-directory . \
  -f ./docker/spark/docker-compose.yaml \
  --env-file $ENV_FILE \
  up spark-master -d

echo "⏳ Waiting for Spark Master to be ready..."
sleep 10

docker compose -p spark \
  --project-directory . \
  -f ./docker/spark/docker-compose.yaml \
  --env-file $ENV_FILE \
  up spark-worker-1 spark-worker-2 spark-jupyter -d

# airflow optional
# echo "🚀 Starting Airflow..."
# docker compose -p airflow \
#   --project-directory . \
#   -f ./docker/airflow/docker-compose.yaml \
#   -f ./docker/airflow/docker-compose.override.yaml \
#   --env-file $ENV_FILE \
#   up -d

echo "🎉 All services started!"