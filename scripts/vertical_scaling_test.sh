#!/bin/bash

source "$(dirname "$0")/cluster.env"

MASTER_IP="${MASTER_IP}"
MASTER_URL="spark://${MASTER_IP}:7077"
WORKER_IPS="${WORKER_IPS}"
JOB_SCRIPT="/home/ubuntu/src/analysis_jobTheo.py"
RESULTS_FILE="scaling_results_tmp.csv"
WORKER_MEMORY="2g"
WORKER_CORES=2

start_worker() {
    local ip=$1
    local target_worker=$2
    echo "  Starting worker on $ip (${WORKER_MEMORY}, ${WORKER_CORES} cores) ..."
    ssh ubuntu@"$ip" '/home/ubuntu/spark/sbin/start-worker.sh '"$MASTER_URL"' -m '"$WORKER_MEMORY"' -c '"$WORKER_CORES" 2>/dev/null

    local timeout=60 waited=0
    while [ $waited -lt $timeout ]; do
        local alive
        alive=$(curl -s "http://${MASTER_IP}:8080/json/" | grep -o '"aliveworkers":[0-9]*' | grep -o '[0-9]*')
        echo "Alive workers = $alive"
        if [ "$alive" -ge "$target_worker" ]; then
            echo "    Worker registered (${alive} alive)"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    echo "WARNING: Timed out waiting for worker on $ip"
    return 1
}

stop_all_workers() {
    echo "Stopping all workers ..."
    for ip in "${WORKER_IPS[@]}"; do
        ssh ubuntu@"$ip" '/home/ubuntu/spark/sbin/stop-worker.sh' 2>/dev/null
        ssh ubuntu@"$ip" "rm -f /tmp/spark-ubuntu-org.apache.spark.deploy.worker.Worker-*.pid" 2>/dev/null
    done

    local timeout=30 waited=0
    while [ $waited -lt $timeout ]; do
        local alive
        alive=$(curl -s "http://${MASTER_IP}:8080/json/" | python3 -c "import sys,json; print(json.load(sys.stdin).get('aliveworkers', 0))" 2>/dev/null || echo "999")
        if [ "$alive" -eq 0 ]; then
            echo "  All workers stopped"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    echo "WARNING: Workers still alive after timeout"
}

# 
# ─── Main ─────────────────────────────────────────────────────────────
echo "============================================"
echo "  Spark Vertical Scaling Benchmark"
echo "============================================"

echo "workers,run,total_cores,time_seconds" > "$RESULTS_FILE"

stop_all_workers

start_worker "${WORKER_IPS[0]}" "1"
sleep 5

for cores in 1 2 
do 
  echo "Running vertical scaling with $cores cores"

  spark-submit ../src/analysis_job_final.py $cores $cores 2g 1

done    

echo "Done"

echo "===== RESULTS ====="
column -t -s',' "$RESULTS_FILE"

stop_all_workers
