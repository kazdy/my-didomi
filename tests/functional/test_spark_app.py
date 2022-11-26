import pytest
from chispa.schema_comparer import assert_schema_equality_ignore_nullable
from chispa.dataframe_comparer import assert_df_equality

from src.main import main
from src.schemas import output_table_schema

@pytest.mark.functional
class TestSparkApp:

    def test_main_has_expected_schema_has_rows(self, spark, tmp_path, input_dataset_path):
        tmp = tmp_path / "tmp"
        tmp_output_dataset_path = f"file://{str(tmp)}/"

        main(input_dataset_path, tmp_output_dataset_path)

        df = spark.read.parquet(tmp_output_dataset_path)
        actual_schema = df.schema

        assert_schema_equality_ignore_nullable(output_table_schema, actual_schema)
        assert df.count() > 0

    def test_main_has_expected_data(self, tmp_path, spark, input_dataset_path, output_dataset_path):
        tmp = tmp_path / "tmp"
        tmp_output_dataset_path = f"file://{str(tmp)}/"

        expected_df = spark.read.schema(output_table_schema).json(output_dataset_path)

        main(input_dataset_path, tmp_output_dataset_path)

        actual_df = spark.read.parquet(tmp_output_dataset_path)

        assert_df_equality(expected_df, actual_df, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)

    def test_main_filter_partition(self, spark, tmp_path, input_dataset_path):
        tmp = tmp_path / "tmp"
        tmp_output_dataset_path = f"file://{str(tmp)}/"

        main(input_dataset_path, tmp_output_dataset_path, "datehour='2021-01-23-10'")

        df = spark.read.parquet(tmp_output_dataset_path)

        assert df.select("datehour").distinct().collect()[0][0] == "2021-01-23-10"