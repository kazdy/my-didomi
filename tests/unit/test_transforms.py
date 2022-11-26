import pytest
from conftest import spark

from pyspark.sql.types import StringType, StructField, StructType

from chispa.schema_comparer import assert_schema_equality_ignore_nullable
from chispa.dataframe_comparer import assert_df_equality

from src.transforms import *
from src.schemas import *


@pytest.mark.unit
class TestTransforms:

    def test_deduplicate_by_event_id(self, spark, input_dataset_path):
        event_ids = ["05551f56-2d63-477f-91cb-e286b1df16fc","097f20f6-afb9-488b-a24b-0a5a76272dd8"]

        df = spark.read.schema(input_file_schema) \
            .json(input_dataset_path) \
            .filter(F.col("id").isin(event_ids))

        df = deduplicate_by_event_id(df)

        assert df.select("id").distinct().count() == 2


    def test_flatten_user(self, spark):
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


    def test_flatten_user(self, spark):
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


    def test_get_user_consent_status(self, spark):
        input_schema = StructType([StructField("user_token", token_column_schema)])
        input_data_true = [{"user_token": {"vendors": {"enabled": ["Vendor1"], "disabled":[]}, "purposes":{"enabled": ["analytics"], "disabled":[]}}}]
        input_data_false = [{"user_token": {"vendors": {"enabled": [], "disabled":[]},"purposes":{"enabled": [], "disabled":[]}}}]
        input_data_false_2 = [{"user_token": {"vendors": {"enabled": [], "disabled":["Vendor1"]},"purposes":{"enabled": [], "disabled":["analytics"]}}}]
        
        df_true = spark.createDataFrame(input_data_true, input_schema)
        df_false_1 = spark.createDataFrame(input_data_false, input_schema)
        df_false_2 = spark.createDataFrame(input_data_false_2, input_schema)

        actual_df_true = get_user_consent_status(df_true)
        actual_df_false_1 = get_user_consent_status(df_false_1)
        actual_df_false_2 = get_user_consent_status(df_false_2)

        assert actual_df_true.select("user_consent").collect()[0][0] == True
        assert actual_df_false_1.select("user_consent").collect()[0][0] == False
        assert actual_df_false_2.select("user_consent").collect()[0][0] == False


    def test_get_datehour_from_datetime(self, spark):
        input_data = [{"datetime": "2021-01-23 10:22:28"}]
        schema = StructType([StructField("datetime", TimestampType())])

        sc = spark.sparkContext
        df = spark.read.schema(schema).json(sc.parallelize([input_data]))

        df = get_datehour_from_datetime(df)

        assert df.select("datehour").collect()[0][0] == "2021-01-23-10"

    def test_calculate_metrics(self, spark):
        expected_df = spark.read.json("tests/data/calculate_metrics/output/*")
        df = spark.read.json("tests/data/calculate_metrics/input/*")

        actual_df = calculate_metrics(df)

        assert_df_equality(expected_df, actual_df, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)

    def test_transform_has_expected_schema(self, spark, input_dataset_path):
        df = spark.read.schema(input_file_schema).json(input_dataset_path)
        df = transform(df)

        actual_schema = df.schema
        assert_schema_equality_ignore_nullable(output_table_schema, actual_schema)

    def test_transform_has_expected_data(self, spark, input_dataset_path, output_dataset_path):
        expected_df = spark.read.schema(output_table_schema).json(output_dataset_path)
        df = spark.read.schema(input_file_schema).json(input_dataset_path)
        actual_df = transform(df)

        assert_df_equality(expected_df, actual_df, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)

