from pyspark.sql import SparkSession

print("[*] Initializing SparkSession...")
spark = SparkSession.builder \
    .appName("TestSparkEnv") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print(f"[OK] Spark Session created! Version: {spark.version}")
print(f"[OK] Master: {spark.sparkContext.master}")

# Test creating a DataFrame
df = spark.createDataFrame([(1, "test"), (2, "spark")], ["id", "val"])
df.show()

spark.stop()
print("[OK] Test passed successfully!")
