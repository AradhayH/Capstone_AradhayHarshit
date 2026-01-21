import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_home = r"C:\spark\spark-3.5.7-bin-hadoop3"
java_home = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.8.101-hotspot"

os.environ["JAVA_HOME"] = java_home
os.environ["SPARK_HOME"] = spark_home
os.environ["HADOOP_HOME"] = spark_home 
os.environ["PATH"] = f"{os.path.join(spark_home, 'bin')};" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Urban Mobility KPI Analytics") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.extraJavaOptions", 
            "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
            "--add-opens=java.base/java.io=ALL-UNNAMED " +
            "--add-opens=java.base/java.util=ALL-UNNAMED") \
    .getOrCreate()

df = spark.read.parquet("data/parquet/*.parquet")

df.printSchema()
df.show(5)

df.createOrReplaceTempView("trips")

# 1. Overall Totals
spark.sql("""
SELECT 
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM trips
""").show()

# 2. Hourly Demand (Note: used 'pickup_hour' if created in previous step, 
# or extract from datetime as you did below)
spark.sql("""
SELECT hour(pickup_datetime) AS hour, COUNT(*) AS trips
FROM trips
GROUP BY hour
ORDER BY trips DESC
""").show()

# 3. Daily Revenue
spark.sql("""
SELECT 
    date(pickup_datetime) AS day,
    SUM(total_amount) AS daily_revenue
FROM trips
GROUP BY day
ORDER BY day
""").show()

# --- Export KPIs ---
kpi_df = spark.sql("""
SELECT 
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_distance
FROM trips
""")

# Writing to JSON 
kpi_df.write.mode("overwrite").json("outputs/spark_sql_kpi_parquet")

print("KPI export successful!")

spark.stop()
