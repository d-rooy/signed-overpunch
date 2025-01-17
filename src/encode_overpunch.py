import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import random  # Import random module
import pandas

# Initialize SparkSession
spark = SparkSession.builder.master("local[*]").appName("OverpunchDecoderTest").getOrCreate()

# Define Overpunch Mapping
overpunch_map = {
    '{': 0, 'A': 1, 'B': 2, 'C': 3, 'D': 4,
    'E': 5, 'F': 6, 'G': 7, 'H': 8, 'I': 9,
    '}': 0, 'J': 1, 'K': 2, 'L': 3, 'M': 4,
    'N': 5, 'O': 6, 'P': 7, 'Q': 8, 'R': 9
}

# Decode Overpunch Function
def decode_overpunch(value: str) -> float:
    if not value:
        return None
    num = int(value[:-1])  # All but the last character
    sign_char = value[-1]
    sign = -1 if sign_char in 'JKLMNOPQR' else 1
    digit = overpunch_map.get(sign_char, 0)
    return sign * (num * 10 + digit)

# Register UDF
decode_udf = udf(decode_overpunch, FloatType())

# Generate Test Data
def encode_overpunch(number):
    if number < 0:
        sign_char = list(overpunch_map.keys())[number % -10]
    else:
        sign_char = list(overpunch_map.keys())[number % 10]
    return f"{abs(number) // 10}{sign_char}"

data = [
    {"id": i + 1, 
     "overpunch_col": encode_overpunch(random.randint(-9999, 9999)), 
     "description": f"Test line {i + 1}"}
    for i in range(20)
]

# Convert to Pandas DataFrame for verification
df_pandas = pd.DataFrame(data)

# Convert to Spark DataFrame
df_spark = spark.createDataFrame(df_pandas)

# Apply UDF to decode the overpunch column
df_decoded = df_spark.withColumn("decoded_value", decode_udf(df_spark["overpunch_col"]))

# Display Results
df_decoded.show()
