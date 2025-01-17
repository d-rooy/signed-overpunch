# notebooks/04_sql_queries.py
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]").appName("SQLQueries").getOrCreate()

# Load Delta table
df = spark.read.format("delta").load("/path/to/delta_table")

# Create a temporary view for SQL queries
df.createOrReplaceTempView("decoded_data")

# Run SQL query
result = spark.sql("SELECT * FROM decoded_data WHERE decoded_value < 0")
result.show()
