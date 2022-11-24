import pytest
from conftest import spark

from pyspark.sql.types import StringType, StructField, StructType

from chispa.schema_comparer import assert_schema_equality_ignore_nullable
from chispa.dataframe_comparer import assert_df_equality

from src.transform import deduplicate_by_event_id, flatten_user, convert_user_token_from_json, get_user_consent_status
from src.schemas import token_column_schema


def test_deduplicate_by_event_id(spark):
    input_data = [
        {"id": "05551f56-2d63-477f-91cb-e286b1df16fc", "datetime": "2021-01-23 10:22:28", "domain": "www.domain-A.eu", "type": "consent.given", "user": {"id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                                                                                                                                                         "country": "FR", "token": "{\"vendors\":{\"enabled\":[\"Vendor1\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}"}},
        {"id": "097f20f6-afb9-488b-a24b-0a5a76272dd8", "datetime": "2021-01-23 10:50:05", "domain": "www.domain-A.eu", "type": "consent.given", "user": {"id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                                                                                                                                                         "country": "FR", "token": "{\"vendors\":{\"enabled\":[\"Vendor1\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}"}},
        {"id": "097f20f6-afb9-488b-a24b-0a5a76272dd8", "datetime": "2021-01-23 10:50:05", "domain": "www.domain-A.eu", "type": "consent.given", "user": {"id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                                                                                                                                                         "country": "FR", "token": "{\"vendors\":{\"enabled\":[\"Vendor1\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}"}}
    ]

    sc = spark.sparkContext
    df = spark.read.json(sc.parallelize([input_data]))
    df = deduplicate_by_event_id(df)

    assert df.select("id").distinct().count() == 2


def test_flatten_user(spark):
    input_data = [
        {"user": {"id": "1705c98b-367c-6d09-a30f-da9e6f4da700",
                  "country": "FR", "token": "string_token_value"}}
    ]

    sc = spark.sparkContext
    df = spark.read.json(sc.parallelize([input_data]))
    actual_df = flatten_user(df)

    expected_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("user_country", StringType(), True),
        StructField("user_token", StringType(), True)])
    expected_data = [("1705c98b-367c-6d09-a30f-da9e6f4da700",
                      "FR", "string_token_value")]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert_schema_equality_ignore_nullable(expected_schema, actual_df.schema)
    assert_df_equality(expected_df, actual_df)


def test_flatten_user(spark):
    input_data = [
        {"user_token": "{\"vendors\":{\"enabled\":[\"Vendor1\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}"},
        {"user_token": "{\"vendors\":{\"enabled\":[],\"disabled\":[]},\"purposes\":{\"enabled\":[],\"disabled\":[]}}"},
        {"user_token": "{\"vendors\":{\"enabled\":[],\"disabled\":[\"Vendor1\"]},\"purposes\":{\"enabled\":[],\"disabled\":[\"analytics\"]}}"},
    ]

    sc = spark.sparkContext
    df = spark.read.json(sc.parallelize([input_data]))
    actual_df = convert_user_token_from_json(df)

    expected_schema = StructType(
        [StructField("user_token", token_column_schema)])
    assert_schema_equality_ignore_nullable(expected_schema, actual_df.schema)


def test_get_user_consent_status(spark):
    input_schema = StructType(
        [StructField("user_token", token_column_schema)])
    input_data_true = [
        {"user_token": {"vendors": {"enabled": ["Vendor1"], "disabled":[]}, "purposes":{
            "enabled": ["analytics"], "disabled":[]}}}]
    input_data_false = [
        {"user_token": {"vendors": {"enabled": [], "disabled":[]},
                        "purposes":{"enabled": [], "disabled":[]}}}]
    input_data_false_2 = [
        {"user_token": {"vendors": {"enabled": [], "disabled":["Vendor1"]},
                        "purposes":{"enabled": [], "disabled":["analytics"]}}}
    ]
    df = spark.createDataFrame(input_data_true, input_schema)
    df_1 = spark.createDataFrame(input_data_false, input_schema)
    df_2 = spark.createDataFrame(input_data_false_2, input_schema)

    actual_df = get_user_consent_status(df)
    actual_df_1 = get_user_consent_status(df_1)
    actual_df_2 = get_user_consent_status(df_2)

    assert actual_df.select("user_consent").collect()[0][0] == True
    assert actual_df_1.select("user_consent").collect()[0][0] == False
    assert actual_df_2.select("user_consent").collect()[0][0] == False


@ pytest.fixture
def test_transform(spark):
    pass
