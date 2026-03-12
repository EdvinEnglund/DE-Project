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

    # Read parquet from HDFS
    df = spark.read.parquet(
        "hdfs:///user/ubuntu/nyc_tlc/final_downloads/"
    ).select("tip_amount", "DOLocationID")

    # Compute average tip per DOLocationID
    result = df.groupBy("DOLocationID").agg(avg("tip_amount").alias("avg_tip"))

    # Load mapping JSON
    with open("data/taxi_zone_IDs.json") as f:
        data = json.load(f)

    # Create broadcast mapping
    b_mapping = {int(k): v["borough"] for k, v in data.items() if k.isdigit()}
    z_mapping = {int(k): v["zone"] for k, v in data.items() if k.isdigit()}

    # Convert mapping to Spark DataFrame
    mapping_df = spark.createDataFrame(
        [(k, b_mapping[k], z_mapping[k]) for k in b_mapping],
        ["DOLocationID", "Borough", "Zone"]
    )

    # Join with result DataFrame
    result = result.join(mapping_df, on="DOLocationID", how="left")

    # Sort by avg_tip descending
    result = result.orderBy(col("avg_tip").desc())

    # Trigger job
    result.collect()

    spark.stop()

if __name__ == "__main__":
    spark_pipeline(workers=WORKERS, executor_cores=EXECUTOR_CORES)