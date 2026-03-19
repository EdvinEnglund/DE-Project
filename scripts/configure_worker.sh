#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

THIS_HOST="$1"
THIS_IP="$2"

echo "[WORKER ${THIS_HOST}] Updating /etc/hosts..."
{
    echo ""
    echo "# Cluster hosts"
    echo "${MASTER_IP} ${MASTER_HOST}"
    for i in "${!WORKER_HOSTS[@]}"; do
        echo "${WORKER_IPS[$i]} ${WORKER_HOSTS[$i]}"
    done
} | sudo tee -a /etc/hosts >/dev/null

echo "[WORKER ${THIS_HOST}] Writing core-site.xml..."
cat >"${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_IP}:9000</value>
  </property>
</configuration>
EOF

echo "[WORKER ${THIS_HOST}] Writing hdfs-site.xml..."
cat >"${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" <<EOF
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
EOF

echo "[WORKER ${THIS_HOST}] Resetting DataNode directory (safe for snapshots)..."
sudo rm -rf "${HDFS_DATA_DIR:?}/"*

echo "[WORKER ${THIS_HOST}] Worker configuration applied."
