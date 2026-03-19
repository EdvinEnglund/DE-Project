#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

echo "[CLUSTER] Stopping any old Spark daemons..."
"${SPARK_HOME}/sbin/stop-workers.sh" || true
"${SPARK_HOME}/sbin/stop-master.sh" || true

echo "[CLUSTER] Starting HDFS..."
"${HADOOP_HOME}/sbin/start-dfs.sh"

echo "[CLUSTER] Starting Spark Master..."
"${SPARK_HOME}/sbin/start-master.sh"

echo "[CLUSTER] Starting Spark Workers..."
"${SPARK_HOME}/sbin/start-workers.sh"

echo "[CLUSTER] HDFS + Spark cluster started successfully!"
``