from pyspark.sql import DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as F

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
    df = df.withColumn("user_token", F.from_json(df.user_token, token_column_schema))
    return df


def get_user_consent_status(df: DataFrame) -> DataFrame:
    df = df.withColumn("user_consent", F.when(F.size(df.user_token.purposes.enabled) > 0, True).otherwise(False))
    df = df.drop("user_token")
    return df


def get_datehour_from_datetime(df: DataFrame) -> DataFrame:
    df = df.withColumn("datehour", F.date_format(df.datetime, "yyyy-MM-dd-HH").cast(StringType()))
    df = df.drop("datetime")
    return df


def calculate_metrics(df: DataFrame) -> DataFrame:
    df = df.groupBy(df.datehour, df.domain, df.user_country).agg(
        F.count_distinct(df.user_id).alias("unique_user_count"),
        F.count(F.when(df.type == "pageview", True)).alias("pageviews"),
        F.count(F.when((df.type == "pageview") & (df.user_consent == True), True)).alias("pageviews_with_consent"),
        F.count(F.when(df.type == "consent.asked", True)).alias("consents_asked"),
        F.count(F.when((df.type == "consent.asked") & (df.user_consent == True), True)).alias("consents_asked_with_consent"),
        F.count(F.when(df.type == "consent.given", True)).alias("consents_given"),
        F.count(F.when((df.type == "consent.given") & (df.user_consent == True), True)).alias("consents_given_with_consent"),
    )
    df = df.withColumn("avg_pageviews_per_user", F.round(F.col("pageviews") / F.col("unique_user_count"),2))
    return df

def apply_output_schema(df: DataFrame) -> DataFrame:
    df = df.select(
        F.col("datehour").cast(StringType()),
        F.col("domain").cast(StringType()),
        F.col("user_country").cast(StringType()).alias("country"),
        F.col("pageviews").cast(LongType()),
        F.col("pageviews_with_consent").cast(LongType()),
        F.col("consents_asked").cast(LongType()),
        F.col("consents_asked_with_consent").cast(LongType()),
        F.col("consents_given").cast(LongType()),
        F.col("consents_given_with_consent").cast(LongType()),
        F.col("avg_pageviews_per_user").cast(DoubleType())
        )
    return df

def transform(df: DataFrame) -> DataFrame:
    df = deduplicate_by_event_id(df)
    df = flatten_user(df)
    df = convert_user_token_from_json(df)
    df = get_user_consent_status(df)
    df = get_datehour_from_datetime(df)
    df = calculate_metrics(df)
    df = apply_output_schema(df)
    return df
