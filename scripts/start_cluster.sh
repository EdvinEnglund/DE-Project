#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

echo "[CLUSTER] Starting HDFS..."
"${HADOOP_HOME}/sbin/start-dfs.sh"

echo "[CLUSTER] Starting Spark Master & Workers..."
"${SPARK_HOME}/sbin/start-master.sh"
"${SPARK_HOME}/sbin/start-workers.sh"

echo "[CLUSTER] Cluster started!"
``