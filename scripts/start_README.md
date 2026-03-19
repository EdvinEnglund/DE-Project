cd ~/DE-Project/scripts

# 0) Edit cluster.env with current private IPs + hostnames
nano cluster.env

# 1) Push scripts to workers
source ./cluster.env
cd ~/DE-Project

for ip in "${WORKER_IPS[@]}"; do
  ssh -o StrictHostKeyChecking=no ubuntu@"$ip" "mkdir -p /home/ubuntu/scripts"
  scp -o StrictHostKeyChecking=no -r scripts/* ubuntu@"$ip":/home/ubuntu/scripts/
done

# 3. Configure master
cd ~/DE-Project/scripts
bash ./configure_master.sh

# 4. Configure workers
for i in 0 1 2; do
  ssh ubuntu@${WORKER_IPS[$i]} "bash ~/scripts/configure_worker.sh ${WORKER_HOSTS[$i]} ${WORKER_IPS[$i]}"
done

# 5. Start the cluster
bash ./start_cluster.sh



# 6) (Optional) Load your dataset to HDFS once
hdfs dfs -mkdir -p /data/input
hdfs dfs -put /path/to/data/*.csv /data/input

# Run your job at 1, 2, 3 workers and collect timings
bash ./run_scale_bench.sh spark_benchmark.py hdfs://<MASTER_IP>:9000/data/input hdfs://<MASTER_IP>:9000/user/ubuntu/bench_out

