from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

# Define Overpunch Decoder
def decode_overpunch(value: str) -> float:
    overpunch_map = {
        '{': 0, 'A': 1, 'B': 2, 'C': 3, 'D': 4,
        'E': 5, 'F': 6, 'G': 7, 'H': 8, 'I': 9,
        '}': 0, 'J': 1, 'K': 2, 'L': 3, 'M': 4,
        'N': 5, 'O': 6, 'P': 7, 'Q': 8, 'R': 9
    }
    if not value:
        return None

    num = int(value[:-1])  # All but the last character
    sign_char = value[-1]
    sign = -1 if sign_char in '}JKLMNOPQR' else 1
    digit = overpunch_map.get(sign_char, 0)

    return sign * (num * 10 + digit)

# Register UDF
decode_udf = udf(decode_overpunch, FloatType())

# Apply UDF to DataFrame
df = spark.read.csv(input_path, schema=schema, sep="|")
df = df.withColumn("decoded_value", decode_udf(df.overpunch_col))

# Display Decoded Data
df.show()
