import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, month, dayofweek, count, sum, round, to_timestamp

spark_home = r"C:\spark\spark-3.5.7-bin-hadoop3"
java_home = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.8.101-hotspot"

os.environ["JAVA_HOME"] = java_home
os.environ["SPARK_HOME"] = spark_home
os.environ["HADOOP_HOME"] = spark_home 
os.environ["PATH"] = f"{os.path.join(spark_home, 'bin')};" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("UrbanMobility_PySpark_ETL") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.driver.extraJavaOptions", 
            "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
            "--add-opens=java.base/java.io=ALL-UNNAMED " +
            "--add-opens=java.base/java.util=ALL-UNNAMED") \
    .getOrCreate()


input_path = "data/cleaned/*.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)



df_transformed = df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
    .withColumn("hour", hour(col("pickup_datetime"))) \
    .withColumn("month", month(col("pickup_datetime"))) \
    .withColumn("day_of_week", dayofweek(col("pickup_datetime")))


monthly_revenue = df_transformed.groupBy("month").agg(sum("total_amount").alias("monthly_revenue"))

df_transformed = df_transformed.withColumn("zone_lat", round(col("pickup_latitude"), 3)) \
                               .withColumn("zone_long", round(col("pickup_longitude"), 3))

demand_by_zone = df_transformed.groupBy("zone_lat", "zone_long").agg(count("*").alias("trip_count"))


congestion_indicators = df_transformed.filter(col("hour").isin([8, 9, 17, 18])) \
    .groupBy("hour", "zone_lat", "zone_long") \
    .agg(count("*").alias("congestion_level"))

high_value_trips = df_transformed.filter(col("total_amount") > 100)


output_base = "data/parquet"

df_transformed.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/trips_optimized.parquet")
monthly_revenue.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/monthly_revenue.parquet")
demand_by_zone.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/demand_by_zone.parquet")
high_value_trips.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/high_value_trips.parquet")


print("\n--- SPARK EXECUTION PLAN (DAG) ---")
df_transformed.explain(extended=True) 

print(f"Success! Scalable ETL complete. Parquet stored at: {output_base}")

import time; time.sleep(120)
spark.stop()