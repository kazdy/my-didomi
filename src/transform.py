from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json
from schemas import token_column_schema


def deduplicate_by_event_id(df: DataFrame) -> DataFrame:
    df = df.drop_duplicates(["id"])
    return df


def flatten_user(df: DataFrame) -> DataFrame:
    df = df.withColumn("user_id", df.user.id) \
        .withColumn("user_country", df.user.country) \
        .withColumn("user_token", df.user.token) \
        .drop("user")
    return df


def convert_user_token_from_json(df: DataFrame) -> DataFrame:
    df = df.withColumn("user_token", from_json(
        df.user_token, token_column_schema))
    return df


def transform(df: DataFrame) -> DataFrame:
    df = deduplicate_by_event_id(df)
    df = flatten_user(df)
    df = convert_user_token_from_json(df)
    return df
