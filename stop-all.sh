#!/bin/bash

set -e

echo "🛑 Stopping all services..."

read -p "⚠️ This will DELETE ALL DATA. Continue? (y/n): " confirm

if [ "$confirm" != "y" ]; then
  echo "❌ Cancelled"
  exit 1
fi

# =========================
# HADOOP
# =========================
echo "=== STOP HADOOP ==="
docker compose -p hadoop -f ./docker/hadoop/docker-compose.yaml down -v

rm -rf ./docker/hadoop/namenode/*
rm -rf ./docker/hadoop/datanode/datanode_1/*
rm -rf ./docker/hadoop/datanode/datanode_2/*

# =========================
# NESSIE + MYSQL
# =========================
echo "=== STOP NESSIE ==="
docker compose -p nessie -f ./docker/nessie/docker-compose.yaml down -v

rm -rf ./docker/nessie/data/*

# =========================
# MONGO
# =========================
echo "=== STOP MONGO ==="
docker compose -p mongo -f ./docker/mongo/docker-compose.yaml down -v

rm -rf ./docker/mongo/data/* ./docker/mongo/data/.[!.]*

# =========================
# SPARK
# =========================
echo "=== STOP SPARK ==="
docker compose -p spark -f ./docker/spark/docker-compose.yaml down -v

# =========================
# AIRFLOW
# =========================
echo "=== STOP AIRFLOW ==="
docker compose -p airflow \
  -f ./docker/airflow/docker-compose.yaml \
  -f ./docker/airflow/docker-compose.override.yaml \
  down -v

rm -rf ./airflow/config/*
rm -rf ./airflow/logs/*
rm -rf ./airflow/plugins/*
rm -rf ./docker/airflow/data/*

# =========================
# CLEAN NETWORK (OPTIONAL)
# =========================
echo "=== CLEAN NETWORK ==="
docker network prune -f

echo "✅ All services stopped and cleaned!"