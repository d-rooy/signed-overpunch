# notebooks/01_data_ingestion.py
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]").appName("DataIngestion").getOrCreate()

# Define input path
input_path = "/path/to/input_data.txt"

# Define schema (if necessary)
schema = "overpunch_col STRING"

# Read the fixed-width input file
df = spark.read.csv(input_path, schema=schema, sep="|")

# Show the loaded data
df.show()

# Save DataFrame to be used in the next step
df.write.mode("overwrite").parquet("/path/to/ingested_data.parquet")
