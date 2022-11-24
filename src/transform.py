from pyspark.sql import DataFrame


def deduplicate_by_event_id(df: DataFrame) -> DataFrame:
    df = df.drop_duplicates(["id"])
    return df


def transform(df: DataFrame) -> DataFrame:
    df = deduplicate_by_event_id(df)
    return df
