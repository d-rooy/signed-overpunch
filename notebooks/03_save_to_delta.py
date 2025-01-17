# notebooks/03_save_to_delta.py
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]").appName("SaveToDelta").getOrCreate()

# Load decoded data
df_decoded = spark.read.parquet("/path/to/decoded_data.parquet")

# Save to Delta Lake
df_decoded.write.format("delta").mode("overwrite").save("/path/to/delta_table")
