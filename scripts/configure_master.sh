#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

# 1) Hostname and /etc/hosts
sudo hostnamectl set-hostname "${MASTER_HOST}"
{
  echo "127.0.0.1 localhost ${MASTER_HOST}"
  echo "${MASTER_IP} ${MASTER_HOST}"
  for i in "${!WORKER_HOSTS[@]}"; do
    echo "${WORKER_IPS[$i]} ${WORKER_HOSTS[$i]}"
  done
} | sudo tee /etc/hosts >/dev/null

# 2) Hadoop core/hdfs config
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
    <name>dfs.namenode.name.dir</name>
    <value>file://${HDFS_NAME_DIR}</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${HDFS_DATA_DIR}</value>
  </property>
</configuration>
EOF

# Hadoop workers file (who will run DataNode)
mkdir -p "${HADOOP_HOME}/etc/hadoop"
printf "%s\n" "${WORKER_HOSTS[@]}" > "${HADOOP_HOME}/etc/hadoop/workers"

# 3) Spark master + workers config
mkdir -p "${SPARK_HOME}/conf"
cat >"${SPARK_HOME}/conf/spark-env.sh" <<EOF
export SPARK_MASTER_HOST=${MASTER_IP}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY}
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY}
EOF

printf "%s\n" "${WORKER_HOSTS[@]}" > "${SPARK_HOME}/conf/workers"

# 4) Format NameNode only if new
if [[ ! -f "${HDFS_NAME_DIR}/current/VERSION" ]]; then
  "${HADOOP_HOME}/bin/hdfs" namenode -format -force -nonInteractive
fi
``