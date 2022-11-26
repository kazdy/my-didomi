import argparse

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

    df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", help="path to input json dataset", required=True)
    parser.add_argument("--output_path", help="path under which output dataset is stored", required=True)
    parser.add_argument("--partition_filter", help="partition filter, accepts sql where statement, ex. datehour='2021-01-23-10', leave empty disable filter", default='True', type=str)
    args = parser.parse_args()

    main(args.input_path, args.output_path, args.partition_filter)
