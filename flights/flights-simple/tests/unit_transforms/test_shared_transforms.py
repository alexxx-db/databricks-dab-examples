import pytest
from pyspark.testing.utils import assertDataFrameEqual
import sys

sys.path.append('./src')

from flights.transforms import shared_transforms


@pytest.fixture(scope="module")
def spark_session():
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        print("No Databricks Connect, build and return local SparkSession")
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


def test_add_metadata_columns_include_time_true(spark_session):
    input_df = spark_session.createDataFrame(
        [("val1",), ("val2",)], ["col1"]
    )
    result_df = shared_transforms.add_metadata_columns(input_df, include_time=True)

    assert "last_updated_time" in result_df.columns
    assert "source_file" in result_df.columns
    assert "last_updated_date" not in result_df.columns


def test_add_metadata_columns_include_time_false(spark_session):
    input_df = spark_session.createDataFrame(
        [("val1",), ("val2",)], ["col1"]
    )
    result_df = shared_transforms.add_metadata_columns(input_df, include_time=False)

    assert "last_updated_date" in result_df.columns
    assert "source_file" in result_df.columns
    assert "last_updated_time" not in result_df.columns
