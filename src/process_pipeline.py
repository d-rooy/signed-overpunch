# src/process_pipeline.py
from pyspark.sql import SparkSession
from decode_overpunch import decode_overpunch

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]").appName("ProcessPipeline").getOrCreate()

# Load raw data
input_path = "/path/to/input_data.txt"
schema = "overpunch_col STRING"
df = spark.read.csv(input_path, schema=schema, sep="|")

# Apply decoding logic
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

decode_udf = udf(decode_overpunch, FloatType())
df_decoded = df.withColumn("decoded_value", decode_udf(df.overpunch_col))

# Save results
df_decoded.write.format("delta").mode("overwrite").save("/path/to/delta_table")
