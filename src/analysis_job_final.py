import sys
import json
import time
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, broadcast

TOTAL_CORES = 2
EXECUTOR_CORES = 2
MEMORY = "2g"
RUNS_PER_CONFIG = 5
RESULTS_FILE = "scaling_results_temp.csv"

MASTER_URL = "spark://192.168.2.37:7077" 
DATA_FILE_PATH = "hdfs:///user/ubuntu/nyc_tlc/final_downloads/"
def run_pipeline(spark):
    """Run the NYC taxi tip analysis pipeline."""

    df = spark.read.parquet(
        DATA_FILE_PATH
    ).select("tip_amount", "DOLocationID")

    with open("/home/ubuntu/data/taxi_zone_IDs.json") as f:
        data = json.load(f)
    b_mapping = {int(k): v["borough"] for k, v in data.items() if k.isdigit()}

    borough_df = spark.createDataFrame(
        [(k, b_mapping[k]) for k in b_mapping],
        ["DOLocationID", "Borough"]
    )

    df_with_borough = df.join(broadcast(borough_df), on="DOLocationID", how="left")

    result = df_with_borough \
        .groupBy("Borough") \
        .agg(avg("tip_amount").alias("avg_tip")) \
        .orderBy(col("avg_tip").desc())

    result.collect()


if __name__ == "__main__":
    total_cores = int(sys.argv[1]) if len(sys.argv) > 1 else TOTAL_CORES
    executor_cores = int(sys.argv[2]) if len(sys.argv) > 2 else EXECUTOR_CORES
    memory = sys.argv[3] if len(sys.argv) > 3 else MEMORY
    num_workers = int(sys.argv[4]) if len(sys.argv) > 4 else 1

    spark = SparkSession.builder \
        .master(MASTER_URL) \
        .appName("analysis-task") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.cores.max", total_cores) \
        .config("spark.executor.memory", memory) \
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 2 * total_cores)
    spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")
    spark.sparkContext.setLogLevel("ERROR")

    for run in range(RUNS_PER_CONFIG + 1):  # +1 for warmup
        start = time.time()
        run_pipeline(spark)
        elapsed = time.time() - start

        if run == 0:
            print(f"  Warmup: {elapsed:.2f}s (discarded)")
        else:
            print(f"  Run {run}: {elapsed:.2f}s")
            with open(RESULTS_FILE, "a") as f:
                writer = csv.writer(f)
                writer.writerow([num_workers, run, total_cores, f"{elapsed:.2f}"])

    spark.stop()
