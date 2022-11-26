from pyspark.sql.types import *

input_file_schema = StructType([
    StructField("id", StringType()),
    StructField("datetime", TimestampType()),
    StructField("country", StringType()),
    StructField("domain", StringType()),
    StructField("type", StringType()),
    StructField("user", StructType([
        StructField("country", StringType()),
        StructField("id", StringType()),
        StructField("token", StringType())
    ]))
])

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

output_table_schema = StructType([
    StructField("datehour", StringType()),
    StructField("domain", StringType()),
    StructField("country", StringType()),
    StructField("pageviews", LongType()),
    StructField("pageviews_with_consent", LongType()),
    StructField("consents_asked", LongType()),
    StructField("consents_asked_with_consent", LongType()),
    StructField("consents_given", LongType()),
    StructField("consents_given_with_consent", LongType()),
    StructField("avg_pageviews_per_user", DoubleType()),
])