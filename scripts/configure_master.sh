#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

echo "[MASTER] Writing /etc/hosts ..."
{
  echo "127.0.0.1 localhost ${MASTER_HOST}"
  echo "${MASTER_IP} ${MASTER_HOST}"
  for i in "${!WORKER_HOSTS[@]}"; do
    echo "${WORKER_IPS[$i]} ${WORKER_HOSTS[$i]}"
  done
} | sudo tee /etc/hosts >/dev/null

echo "[MASTER] Writing core-site.xml ..."
cat >"${HADOOP_HOME}/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_IP}:9000</value>
  </property>
</configuration>
EOF

echo "[MASTER] Writing hdfs-site.xml ..."
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

echo "[MASTER] Writing Hadoop workers ..."
printf "%s\n" "${WORKER_HOSTS[@]}" > "${HADOOP_HOME}/etc/hadoop/workers"

echo "[MASTER] Writing Spark config ..."
cat >"${SPARK_HOME}/conf/spark-env.sh" <<EOF
export SPARK_MASTER_HOST=${MASTER_IP}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY}
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY}
EOF

printf "%s\n" "${WORKER_HOSTS[@]}" > "${SPARK_HOME}/conf/workers"

echo "[MASTER] Config applied!"
``