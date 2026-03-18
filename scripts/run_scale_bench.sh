#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

JOB="${1:-spark_benchmark.py}"        # your job file path
INPUT="${2:-${HDFS_NN_RPC}/data/input}"   # e.g. hdfs://<ip>:9000/path
OUTBASE="${3:-${HDFS_NN_RPC}/user/${USER}/bench_out}"
CSV="bench_results.csv"

echo "workers,seconds" > "${CSV}"

for N in 1 2 3; do
  "$(dirname "$0")/set_worker_count.sh" "${N}"

  # Clean output for this run
  OUT="${OUTBASE}/w${N}"
  "${HADOOP_HOME}/bin/hdfs" dfs -rm -r -f "${OUT}" >/dev/null 2>&1 || true

  # Run the job
  echo "Running with ${N} worker(s)..."
  # Using Python entry so the same script can be used
  RUNTIME=$(
    python3 "${JOB}" "${INPUT}" "${OUT}" \
      --master "${SPARK_MASTER_URL}" 2>&1 \
      | awk -F= '/BENCH_RUNTIME_SECONDS/ {print $2}' | tail -1
  )

  # Fallback if not captured from stdout
  if [[ -z "${RUNTIME}" ]]; then
    # naive timing via /usr/bin/time
    RUNTIME=$( ( /usr/bin/time -f "%e" spark-submit --master "${SPARK_MASTER_URL}" "${JOB}" "${INPUT}" "${OUT}" ) 2>&1 >/dev/null )
  fi

  echo "${N},${RUNTIME}" | tee -a "${CSV}"
done

echo "Results written to ${CSV}"