#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

# Start NameNode on master
"${HADOOP_HOME}/sbin/stop-dfs.sh" || true
"${HADOOP_HOME}/sbin/start-dfs.sh"

# Start Spark master
"${SPARK_HOME}/sbin/stop-master.sh" || true
"${SPARK_HOME}/sbin/start-master.sh"

# Start Spark workers via conf/workers
"${SPARK_HOME}/sbin/stop-workers.sh" || true
"${SPARK_HOME}/sbin/start-workers.sh"