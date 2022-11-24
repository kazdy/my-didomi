from pyspark.sql import SparkSession

from transform import transform

spark = SparkSession.builder \
    .master("local") \
    .appName("my-didomi") \
    .getOrCreate()

df = spark.read.json(
    "file:///Users/kazdy/workspace/my-didomi/tests/data/input/*")

df.show()

df = transform(df)

df.show()
