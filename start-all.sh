#!/bin/bash

set -e

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────
ENV_FILE=".env.global"
NAMENODE_CONTAINER="namenode"

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
log()     { echo "➡️  $*"; }
success() { echo "✅ $*"; }
warn()    { echo "⚠️  $*"; }
error()   { echo "❌ $*" >&2; exit 1; }

wait_for() {
  local label=$1
  local seconds=$2
  echo "⏳ Waiting ${seconds}s for ${label} to be ready..."
  sleep "$seconds"
}

compose_up() {
  local project=$1
  local compose_file=$2
  shift 2
  docker compose -p "$project" \
    --project-directory . \
    -f "$compose_file" \
    --env-file "$ENV_FILE" \
    up -d "$@"
}

# ─────────────────────────────────────────
# NETWORKS
# ─────────────────────────────────────────
log "Creating networks (if not exist)..."
for net in hadoop_net spark_net airflow_net mongo_net nessie_net data_eng_net; do
  docker network create "$net" 2>/dev/null || true
done
success "Networks ready"

# ─────────────────────────────────────────
# NESSIE — Postgres Backend
# ─────────────────────────────────────────
log "Starting PostgreSQL (Nessie backend)..."
compose_up nessie ./docker/nessie/docker-compose.yaml postgres-backend
wait_for "Nessie Postgres Backend" 60

# ─────────────────────────────────────────
# NESSIE — REST Catalog
# ─────────────────────────────────────────
log "Starting Nessie REST Catalog..."
compose_up nessie ./docker/nessie/docker-compose.yaml nessie-rest-catalog

# ─────────────────────────────────────────
# HADOOP
# ─────────────────────────────────────────
log "Starting Hadoop..."
compose_up hadoop ./docker/hadoop/docker-compose.yaml
wait_for "Hadoop" 10

log "Creating HDFS directories..."
docker exec "$NAMENODE_CONTAINER" bash -c "
  hdfs dfs -mkdir -p /warehouse_dev &&
  hdfs dfs -mkdir -p /warehouse
"

# ─────────────────────────────────────────
# HDFS SAFE MODE CHECK (via docker exec)
# ─────────────────────────────────────────
log "Checking HDFS Safe Mode..."
SAFEMODE=$(docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -safemode get 2>/dev/null)

if echo "$SAFEMODE" | grep -q "ON"; then
  warn "Safe mode is ON, checking HDFS health..."

  MISSING=$(docker exec "$NAMENODE_CONTAINER" \
    bash -c "hdfs dfsadmin -report 2>/dev/null | grep 'Missing blocks:' | awk '{print \$3}'"
  )
  CORRUPT=$(docker exec "$NAMENODE_CONTAINER" \
    bash -c "hdfs dfsadmin -report 2>/dev/null | grep 'Blocks with corrupt replicas:' | awk '{print \$5}'"
  )

  if [ "$MISSING" = "0" ] && [ "$CORRUPT" = "0" ]; then
    log "HDFS is healthy, leaving safe mode..."
    docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -safemode leave
    success "Safe mode OFF, proceeding..."
  else
    error "HDFS has issues! Missing: $MISSING, Corrupt: $CORRUPT — fix HDFS first."
  fi
else
  success "Safe mode is OFF, proceeding..."
fi

# ─────────────────────────────────────────
# MONGO
# ─────────────────────────────────────────
log "Starting MongoDB..."
compose_up mongo ./docker/mongo/docker-compose.yaml mongo-db
wait_for "MongoDB" 60

log "Starting Mongo Express..."
compose_up mongo ./docker/mongo/docker-compose.yaml mongo-express

# ─────────────────────────────────────────
# SPARK
# ─────────────────────────────────────────
log "Starting Spark Master..."
compose_up spark ./docker/spark/docker-compose.yaml spark-master
wait_for "Spark Master" 10

log "Starting Spark Workers & Jupyter..."
compose_up spark ./docker/spark/docker-compose.yaml spark-worker-1 spark-worker-2 spark-jupyter

# ─────────────────────────────────────────
# AIRFLOW (optional)
# ─────────────────────────────────────────
# log "Starting Airflow..."
# docker compose -p airflow \
#   --project-directory . \
#   -f ./docker/airflow/docker-compose.yaml \
#   -f ./docker/airflow/docker-compose.override.yaml \
#   --env-file "$ENV_FILE" \
#   up -d

success "All services started! 🎉"