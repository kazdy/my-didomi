from chispa.schema_comparer import assert_schema_equality_ignore_nullable

from src.main import main
from src.schemas import output_table_schema

def test_main_has_expected_schema_has_rows(spark, tmp_path, input_dataset_path):
    tmp = tmp_path / "tmp"
    output_dataset_path = f"file://{str(tmp)}/"

    main(input_dataset_path, output_dataset_path)

    df = spark.read.parquet(output_dataset_path)
    actual_schema = df.schema

    assert_schema_equality_ignore_nullable(output_table_schema, actual_schema)
    assert df.count() > 0

def test_main_filter_partition(spark, tmp_path, input_dataset_path):
    tmp = tmp_path / "tmp"
    output_dataset_path = f"file://{str(tmp)}/"

    main(input_dataset_path, output_dataset_path, "datehour='2021-01-23-10'")

    df = spark.read.parquet(output_dataset_path)

    assert df.select("datehour").distinct().collect()[0][0] == "2021-01-23-10"