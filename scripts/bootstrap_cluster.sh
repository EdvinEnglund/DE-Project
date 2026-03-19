#!/usr/bin/env bash
set -euo pipefail

# Load environment configuration
source "$(dirname "$0")/cluster.env"

SCRIPT_DIR="$(dirname "$0")"
REMOTE_DIR="/home/${REMOTE_USER}/scripts"

echo "========================================"
echo "  SNAPSHOT-SAFE HDFS + SPARK BOOTSTRAP"
echo "========================================"
echo ""

###############################################
# 1. DISTRIBUTE SCRIPTS TO WORKERS
###############################################
echo "[BOOTSTRAP] Syncing scripts to workers..."
for ip in "${WORKER_IPS[@]}"; do
    ssh $SSH_OPTS "${REMOTE_USER}@${ip}" "mkdir -p ${REMOTE_DIR}"
    scp $SSH_OPTS -r "${SCRIPT_DIR}/"*.sh "${SCRIPT_DIR}/cluster.env" "${REMOTE_USER}@${ip}:${REMOTE_DIR}/"
done


###############################################
# FUNCTIONS USED BELOW
###############################################

update_hosts() {
    local host_label="$1"
    echo "[${host_label}] Updating /etc/hosts..."
    {
        echo ""
        echo "# Cluster hosts"
        echo "${MASTER_IP} ${MASTER_HOST}"
        for i in "${!WORKER_HOSTS[@]}"; do
            echo "${WORKER_IPS[$i]} ${WORKER_HOSTS[$i]}"
        done
    } | sudo tee -a /etc/hosts >/dev/null
}

configure_master() {
    echo "[MASTER] Applying master configuration..."

    update_hosts "MASTER"

    # OPTIONAL NN FORMAT CLEAN (uncomment if snapshot contains stale metadata)
    # echo "[MASTER] Resetting NameNode directory..."
    # sudo rm -rf "${HDFS_NAME_DIR:?}/"*

    echo "[MASTER] Writing Hadoop configs..."

    cat >"${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_IP}:9000</value>
  </property>
</configuration>
EOF

    cat >"${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>${DFS_REPLICATION}</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://${HDFS_NAME_DIR}</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${HDFS_DATA_DIR}</value>
  </property>
</configuration>
EOF

    printf "%s\n" "${WORKER_HOSTS[@]}" > "${HADOOP_HOME}/etc/hadoop/workers"

    echo "[MASTER] Writing Spark config..."
    cat > "${SPARK_HOME}/conf/spark-env.sh" <<EOF
export SPARK_MASTER_HOST=${MASTER_IP}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY}
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY}
EOF

    printf "%s\n" "${WORKER_HOSTS[@]}" > "${SPARK_HOME}/conf/workers"

    echo "[MASTER] Master configuration complete."
}

configure_worker_remote() {
    local host="$1"
    local ip="$2"

ssh $SSH_OPTS "${REMOTE_USER}@${ip}" bash <<EOF
set -euo pipefail
source "${REMOTE_DIR}/cluster.env"

echo "[WORKER ${host}] Starting configuration..."

{
    echo ""
    echo "# Cluster hosts"
    echo "${MASTER_IP} ${MASTER_HOST}"
    for i in "\${!WORKER_HOSTS[@]}"; do
        echo "\${WORKER_IPS[\$i]} \${WORKER_HOSTS[\$i]}"
    done
} | sudo tee -a /etc/hosts >/dev/null

echo "[WORKER ${host}] Writing Hadoop configs..."

cat >"\${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOCFG
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_IP}:9000</value>
  </property>
</configuration>
EOCFG

cat >"\${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" <<EOCFG
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>${DFS_REPLICATION}</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${HDFS_DATA_DIR}</value>
  </property>
</configuration>
EOCFG

echo "[WORKER ${host}] Resetting DataNode directory..."
sudo rm -rf "${HDFS_DATA_DIR:?}/"*

echo "[WORKER ${host}] Done."
EOF
}


###############################################
# 2. CONFIGURE MASTER NODE
###############################################
echo "[BOOTSTRAP] Configuring master node..."
configure_master


###############################################
# 3. CONFIGURE WORKER NODES
###############################################
echo "[BOOTSTRAP] Configuring workers..."
for i in "${!WORKER_HOSTS[@]}"; do
    configure_worker_remote "${WORKER_HOSTS[$i]}" "${WORKER_IPS[$i]}"
done


###############################################
# 4. START HDFS + SPARK
###############################################
echo "[BOOTSTRAP] Stopping any stale Spark services..."
"${SPARK_HOME}/sbin/stop-workers.sh" || true
"${SPARK_HOME}/sbin/stop-master.sh" || true

echo "[BOOTSTRAP] Starting HDFS..."
"${HADOOP_HOME}/sbin/start-dfs.sh"

echo "[BOOTSTRAP] Starting Spark master..."
"${SPARK_HOME}/sbin/start-master.sh"

echo "[BOOTSTRAP] Starting Spark workers..."
"${SPARK_HOME}/sbin/start-workers.sh"

echo "========================================"
echo "  CLUSTER READY!"
echo "  HDFS: hdfs://${MASTER_IP}:9000"
echo "  SPARK MASTER: ${SPARK_MASTER_URL}"
echo "========================================"