import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from config import SPARK_MASTER, HDFS_DATA_PATH, WORKERS, EXECUTOR_CORES, MEMORY


def spark_pipeline(workers=WORKERS, executor_cores=EXECUTOR_CORES, memory=MEMORY):
    spark = SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName("analysis-task") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.instances", workers) \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.executor.memory", memory) \
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 2 * workers * executor_cores)
    spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")

    df = spark.read.parquet(HDFS_DATA_PATH).select("tip_amount", "DOLocationID")
    result = df.groupBy("DOLocationID").agg(avg("tip_amount").alias("avg_tip"))

    result.collect()

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi Analysis Job")
    parser.add_argument("--workers",        type=int, default=WORKERS)
    parser.add_argument("--executor-cores", type=int, default=EXECUTOR_CORES)
    parser.add_argument("--memory",         type=str, default=MEMORY)
    args = parser.parse_args()

    spark_pipeline(
        workers=args.workers,
        executor_cores=args.executor_cores,
        memory=args.memory,
    )

