from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from transforms import transform
from schemas import input_file_schema


def main(input_path: str, output_path: str, partition_filter: str = 'True'):
    spark = SparkSession.builder.getOrCreate()

    df = spark.read \
        .schema(input_file_schema) \
        .json(input_path) \
        .filter(partition_filter)

    df = transform(df)

    df.write.parquet(output_path)


if __name__ == "__main__":
    main(None, None)
