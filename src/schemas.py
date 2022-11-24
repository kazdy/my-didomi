from pyspark.sql.types import ArrayType, StringType, StructField, StructType

input_file_schema = None

token_column_schema = StructType([
    StructField("vendors", StructType([
        StructField("enabled", ArrayType(StringType()), True),
        StructField("disabled", ArrayType(StringType()), True)
    ]), True),
    StructField("purposes", StructType([
        StructField("enabled", ArrayType(StringType()), True),
        StructField("disabled", ArrayType(StringType()), True)
    ]), True)
])
