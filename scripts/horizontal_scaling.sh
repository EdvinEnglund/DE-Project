#!/bin/bash
set -o pipefail

source "$(dirname "$0")/cluster.env"

start_worker() {
    local ip=$1
    local target_workers=$2
    echo "  Starting worker on $ip (${SPARK_WORKER_MEMORY}, ${SPARK_WORKER_CORES} cores) ..."
    ssh ubuntu@"$ip" '/home/ubuntu/spark/sbin/start-worker.sh '"$SPARK_MASTER_URL"' -m '"$SPARK_WORKER_MEMORY"' -c '"$SPARK_WORKER_CORES" 2>/dev/null
    #Timeout function, waiting for worker to register
    local timeout=60 waited=0 #Wait for 60 seconds
    while [ $waited -lt $timeout ]; do
        local alive
        #Query spark masters REST API to get current number of alive workers
        alive=$(curl -s "http://${MASTER_IP}:8080/json/" | python3 -c "import sys,json; print(json.load(sys.stdin).get('aliveworkers', 0))" 2>/dev/null || echo "0")
        #Check if enough workers have registred
        if [ "$alive" -ge "$target_workers" ]; then
            echo "    Worker registered (${alive} alive)"
            return 0 
        fi
        sleep 2  #Wait 2 seconds before checking again
        waited=$((waited + 2))  #Increment the wait counter
    done
    echo "    WARNING: Timed out waiting for worker on $ip"
    return 1
}
#Stop all workers in the cluster
stop_all_workers() {
    echo "Stopping all workers ..."
    for ip in "${WORKER_IPS[@]}"; do
       #Stop each worker by SSH-ing  each worker
        ssh ubuntu@"$ip" '/home/ubuntu/spark/sbin/stop-worker.sh' 2>/dev/null
        ssh ubuntu@"$ip" "rm -f /tmp/spark-ubuntu-org.apache.spark.deploy.worker.Worker-*.pid" 2>/dev/null
    done
    #Same logic as start_worker()
    local timeout=60 waited=0
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
    echo "  WARNING: Workers still alive after timeout"
}

# ─── Main ─────────────────────────────────────────────────────────────
echo "============================================"
echo "  Spark Horizontal Scaling Benchmark"
echo "============================================"

echo "workers,run,total_cores,time_seconds" > "$RESULTS_FILE"

stop_all_workers

#For 1, 2, 3 workers run the analysis job.
for n in 1 2 3; do
    echo "========== TEST: $n worker(s) =========="
    start_worker "${WORKER_IPS[$((n-1))]}" "$n"

    total_cores=$((n * SPARK_WORKER_CORES))
    python3 "$JOB_SCRIPT" "$total_cores" "$SPARK_WORKER_CORES" "$SPARK_WORKER_MEMORY" "$n"

    echo ""
done

echo "===== RESULTS ====="
column -t -s',' "$RESULTS_FILE"

stop_all_workers
