#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

THIS_WORKER_HOST="$1"
THIS_WORKER_IP="$2"

# 1) Hostname and /etc/hosts
sudo hostnamectl set-hostname "${THIS_WORKER_HOST}"
{
  echo "127.0.0.1 localhost ${THIS_WORKER_HOST}"
  echo "${MASTER_IP} ${MASTER_HOST}"
  for i in "${!WORKER_HOSTS[@]}"; do
    echo "${WORKER_IPS[$i]} ${WORKER_HOSTS[$i]}"
  done
} | sudo tee /etc/hosts >/dev/null

# 2) Hadoop config (point to master)
cat >"${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>${HDFS_NN_RPC}</value>
  </property>
</configuration>
EOF

cat >"${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
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

# 3) Spark config (workers inherit master host from spark-submit)
mkdir -p "${SPARK_HOME}/conf"
cat >"${SPARK_HOME}/conf/spark-env.sh" <<EOF
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
EOF

# 4) One-time reset of DataNode data after snapshot
if [[ ! -f "${HDFS_DATA_DIR}/current/VERSION" ]]; then
  sudo rm -rf "${HDFS_DATA_DIR:?}/"*
fi
``