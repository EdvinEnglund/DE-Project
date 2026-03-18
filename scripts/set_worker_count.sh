#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

N="${1:-${DEFAULT_WORKER_COUNT}}"
if (( N < 1 || N > ${#WORKER_HOSTS[@]} )); then
  echo "Usage: $0 <1..${#WORKER_HOSTS[@]}>" >&2
  exit 1
fi

SELECTED=( "${WORKER_HOSTS[@]:0:${N}}" )

printf "%s\n" "${SELECTED[@]}" > "${HADOOP_HOME}/etc/hadoop/workers"
printf "%s\n" "${SELECTED[@]}" > "${SPARK_HOME}/conf/workers"

# Restart services to apply
"$(dirname "$0")/stop_cluster.sh"
"$(dirname "$0")/start_cluster.sh"

echo "Cluster now running with ${N} worker(s): ${SELECTED[*]}"