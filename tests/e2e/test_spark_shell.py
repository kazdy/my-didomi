import subprocess
import logging

import pytest
from chispa.schema_comparer import assert_schema_equality_ignore_nullable
from chispa.dataframe_comparer import assert_df_equality

from src.schemas import output_table_schema

pytestmark = pytest.mark.e2e

@pytest.mark.e2e
class TestAppSparkShell:
    def test_has_expected_schema(self, spark, tmp_path, caplog, input_dataset_path):
        caplog.set_level(logging.INFO)

        tmp = tmp_path / "tmp"

        tmp_output_dataset_path = f"file://{str(tmp)}/"
        filter = "True"

        spark_submit_str= f"""
        spark-submit \
            --master local \
            --deploy-mode client \
            --py-files src/schemas.py,src/transforms.py \
            src/main.py --input_path {input_dataset_path} --output_path {tmp_output_dataset_path} --partition_filter {filter}
        """
        process=subprocess.Popen(spark_submit_str, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout,stderr = process.communicate(timeout=60)
        
        if process.returncode != 0:
            logging.getLogger().info("spark-shell error: %s", stderr)

        actual_df = spark.read.parquet(tmp_output_dataset_path)
        actual_schema = actual_df.schema

        assert process.returncode == 0
        assert_schema_equality_ignore_nullable(output_table_schema, actual_schema)



    def test_has_expected_data(self, caplog, tmp_path, spark, input_dataset_path, output_dataset_path):
        caplog.set_level(logging.INFO)

        tmp = tmp_path / "tmp"

        tmp_output_dataset_path = f"file://{str(tmp)}/"
        filter = "True"

        spark_submit_str= f"""
        spark-submit \
            --master local \
            --deploy-mode client \
            --py-files src/schemas.py,src/transforms.py \
            src/main.py --input_path {input_dataset_path} --output_path {tmp_output_dataset_path} --partition_filter {filter}
        """
        process=subprocess.Popen(spark_submit_str, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout,stderr = process.communicate(timeout=60)
        
        if process.returncode != 0:
            logging.getLogger().info("spark-shell error: %s", stderr)

        actual_df = spark.read.parquet(tmp_output_dataset_path)
        expected_df = spark.read.schema(output_table_schema).json(output_dataset_path)

        assert_df_equality(expected_df, actual_df, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)
