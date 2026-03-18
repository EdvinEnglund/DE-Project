#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

"${SPARK_HOME}/sbin/stop-workers.sh" || true
"${SPARK_HOME}/sbin/stop-master.sh" || true
"${HADOOP_HOME}/sbin/stop-dfs.sh" || true