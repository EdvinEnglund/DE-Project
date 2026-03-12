import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Change for horizontal scaling
WORKERS = 3

# Change for vertical scaling
EXECUTOR_CORES = 2
MEMORY = "2g" # (max 2.8g)

def spark_pipeline(workers = WORKERS, executor_cores = EXECUTOR_CORES, memory = MEMORY):
    spark = SparkSession.builder \
        .master("spark://192.168.2.37:7077") \
        .appName("analysis-task") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.instances", workers) \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.executor.memory", memory) \
        .getOrCreate()

    # Shuffle optimization suggested by ChatGPT
    spark.conf.set("spark.sql.shuffle.partitions", 2 * WORKERS * EXECUTOR_CORES)
    spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")

    # Create pipe for loading and analyzing data
    df = spark.read.parquet("hdfs:///user/ubuntu/nyc_tlc/final_downloads/").select("tip_amount", "DOLocationID")
    result = df.groupBy("DOLocationID").agg(avg("tip_amount").alias("avg_tip"))

    # Trigger job
    result.collect()

    spark.stop()

if __name__ == "__main__":
    spark_pipeline(workers=WORKERS, executor_cores=EXECUTOR_CORES)