import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit, broadcast
from pyspark.sql.types import StringType

# Change for horizontal scaling
TOTAL_CORES = 3

# Change for vertical scaling
EXECUTOR_CORES = 1
MEMORY = "2g"  # (max 2.8g)

def spark_pipeline(total_cores = TOTAL_CORES, executor_cores = EXECUTOR_CORES, memory = MEMORY):
    # Build spark session
    spark = SparkSession.builder \
        .master("spark://192.168.2.37:7077") \
        .appName("analysis-task") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.cores.max", total_cores) \
        .config("spark.executor.memory", memory) \
        .getOrCreate()

    # Shuffle optimization
    spark.conf.set("spark.sql.shuffle.partitions", 2 * total_cores)
    spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")

    # Ignore warnings...
    spark.sparkContext.setLogLevel("ERROR")

    # Load parquet data
    df = spark.read.parquet(
        "hdfs:///user/ubuntu/nyc_tlc/final_downloads/"
    ).select("tip_amount", "DOLocationID")

    # Load mapping JSON (only boroughs needed)
    with open("data/taxi_zone_IDs.json") as f:
        data = json.load(f)
    b_mapping = {int(k): v["borough"] for k, v in data.items() if k.isdigit()}

    # Convert mapping to Spark DataFrame
    borough_df = spark.createDataFrame(
        [(k, b_mapping[k]) for k in b_mapping],
        ["DOLocationID", "Borough"]
    )

    # Broadcast Join taxi data with borough mapping
    df_with_borough = df.join(broadcast(borough_df), on="DOLocationID", how="left")

    # Group by borough and compute average tip
    result = df_with_borough.groupBy("Borough").agg(avg("tip_amount").alias("avg_tip"))

    # Sort by avg_tip descending
    result = result.orderBy(col("avg_tip").desc())

    # Trigger job
    result.collect()

    spark.stop()

if __name__ == "__main__":
    spark_pipeline(total_cores=TOTAL_CORES, executor_cores=EXECUTOR_CORES, memory=MEMORY)