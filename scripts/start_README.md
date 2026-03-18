cd project-repo/scripts

# 0) Edit cluster.env with current private IPs + hostnames
nano cluster.env

# 1) Push scripts to workers & bootstrap all nodes
for i in 0 1 2; do
  scp ${SSH_OPTS} -r . ${REMOTE_USER}@${WORKER_IPS[$i]}:/home/${REMOTE_USER}/scripts
  ssh ${SSH_OPTS} ${REMOTE_USER}@${WORKER_IPS[$i]} "bash ~/scripts/bootstrap_common.sh"
done

# 2) Bootstrap on master
bash ./bootstrap_common.sh

# 3) Configure master
bash ./configure_master.sh

# 4) Configure each worker with its own hostname/IP
for i in 0 1 2; do
  ssh ${SSH_OPTS} ${REMOTE_USER}@${WORKER_IPS[$i]} "bash ~/scripts/configure_worker.sh ${WORKER_HOSTS[$i]} ${WORKER_IPS[$i]}"
done

# 5) Start full cluster (HDFS + Spark)
bash ./start_cluster.sh

# 6) (Optional) Load your dataset to HDFS once
# hdfs dfs -mkdir -p /data/input
# hdfs dfs -put /path/to/data/*.csv /data/input

# Run your job at 1, 2, 3 workers and collect timings
bash ./run_scale_bench.sh spark_benchmark.py hdfs://<MASTER_IP>:9000/data/input hdfs://<MASTER_IP>:9000/user/ubuntu/bench_out

