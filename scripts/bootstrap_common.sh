#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/cluster.env"

sudo apt-get update -y
sudo apt-get install -y ${JAVA_PKG} curl tar rsync

# Create dirs
sudo mkdir -p "${INSTALL_DIR}" "${HDFS_NAME_DIR}" "${HDFS_DATA_DIR}"
sudo chown -R "${USER}:${USER}" "${INSTALL_DIR}" "${HDFS_NAME_DIR}" "${HDFS_DATA_DIR}"

# Hadoop
if [[ ! -d "${HADOOP_HOME}" ]]; then
  curl -L "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | sudo tar -xz -C "${INSTALL_DIR}"
fi

# Spark (pre-built for Hadoop 3)
if [[ ! -d "${SPARK_HOME}" ]]; then
  curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    | sudo tar -xz -C "${INSTALL_DIR}"
  # Normalize path name (Spark untars with suffix)
  if [[ -d "${INSTALL_DIR}/spark-${SPARK_VERSION}-bin-hadoop3" ]]; then
    sudo mv "${INSTALL_DIR}/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}"
  fi
fi

# Bash profile for this user
grep -q "HADOOP_HOME=" ~/.bashrc || cat >>~/.bashrc <<EOF

# Hadoop/Spark env
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=${HADOOP_HOME}
export SPARK_HOME=${SPARK_HOME}
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOF
``