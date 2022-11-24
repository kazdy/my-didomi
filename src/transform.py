from pyspark.sql import DataFrame


def deduplicate_by_event_id(df: DataFrame) -> DataFrame:
    df = df.drop_duplicates(["id"])
    return df


def flatten_user(df: DataFrame) -> DataFrame:
    df = df.withColumn("user_id", df.user.id) \
        .withColumn("user_country", df.user.country) \
        .withColumn("user_token", df.user.token) \
        .drop("user")
    return df


def transform(df: DataFrame) -> DataFrame:
    df = deduplicate_by_event_id(df)
    df = flatten_user(df)
    return df
