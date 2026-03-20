#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/cluster.env"

SCRIPT_DIR="$(dirname "$0")"
REMOTE_DIR="/home/${REMOTE_USER}/scripts"

echo "========================================"
echo "  UPDATED SNAPSHOT-SAFE CLUSTER BOOTSTRAP"
echo "========================================"


###############################################
# 1. PREPARE MASTER DIRECTORIES
###############################################
echo "[MASTER] Creating HDFS directories..."

sudo mkdir -p "${HDFS_NAME_DIR}"
sudo mkdir -p "${HDFS_DATA_DIR}"
sudo chown -R "${REMOTE_USER}:${REMOTE_USER}" /data/hdfs

echo "[MASTER] HDFS directories created and permissions fixed."


###############################################
# 2. DISTRIBUTE ENV TO WORKERS
###############################################
echo "[BOOTSTRAP] Syncing ENV to workers..."

for ip in "${WORKER_IPS[@]}"; do
    ssh $SSH_OPTS "${REMOTE_USER}@${ip}" "mkdir -p ${REMOTE_DIR}"

    scp $SSH_OPTS \
        "${SCRIPT_DIR}/cluster.env" \
        "${REMOTE_USER}@${ip}:${REMOTE_DIR}/"
done


###############################################
# FUNCTIONS
###############################################

update_hosts() {
    local node="$1"

    echo "[${node}] Updating /etc/hosts..."
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
    echo "[MASTER] Applying configuration..."

    update_hosts "MASTER"

    echo "[MASTER] Writing Hadoop configs..."

    cat > "${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_IP}:9000</value>
  </property>
</configuration>
EOF

    cat > "${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" <<EOF
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

echo "[WORKER ${host}] Creating HDFS directories..."
sudo mkdir -p "${HDFS_DATA_DIR}"
sudo chown -R "${REMOTE_USER}:${REMOTE_USER}" /data/hdfs

echo "[WORKER ${host}] Updating /etc/hosts..."
{
    echo ""
    echo "# Cluster hosts"
    echo "${MASTER_IP} ${MASTER_HOST}"
    for i in "\${!WORKER_HOSTS[@]}"; do
        echo "\${WORKER_IPS[\$i]} \${WORKER_HOSTS[\$i]}"
    done
} | sudo tee -a /etc/hosts >/dev/null

echo "[WORKER ${host}] Writing Hadoop configs..."
cat > "\${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOC
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_IP}:9000</value>
  </property>
</configuration>
EOC

cat > "\${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" <<EOC
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
EOC

echo "[WORKER ${host}] Resetting datanode directory..."
sudo rm -rf "${HDFS_DATA_DIR:?}/"*

echo "[WORKER ${host}] Done."
EOF
}


###############################################
# 3. CONFIGURE MASTER & WORKERS
###############################################

echo "[BOOTSTRAP] Configuring master..."
configure_master

echo "[BOOTSTRAP] Configuring workers..."
for i in "${!WORKER_HOSTS[@]}"; do
    configure_worker_remote "${WORKER_HOSTS[$i]}" "${WORKER_IPS[$i]}"
done


###############################################
# 4. START SERVICES
###############################################
echo "[BOOTSTRAP] Stopping old Spark processes..."
"${SPARK_HOME}/sbin/stop-workers.sh" || true
"${SPARK_HOME}/sbin/stop-master.sh" || true

echo "[BOOTSTRAP] Formatting NameNode (only if empty)..."
if [ ! -d "${HDFS_NAME_DIR}/current" ]; then
    hdfs namenode -format -force
fi

echo "[BOOTSTRAP] Starting HDFS..."
"${HADOOP_HOME}/sbin/start-dfs.sh"

echo "[BOOTSTRAP] Starting Spark Master..."
"${SPARK_HOME}/sbin/start-master.sh"

echo "[BOOTSTRAP] Starting Spark Workers..."
"${SPARK_HOME}/sbin/start-workers.sh"

echo ""
echo "========================================"
echo "  CLUSTER READY!"
echo "========================================"