import pytest
import tempfile
import os
import shutil
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


@pytest.fixture
def file_backed_df(spark_session, tmp_path):
    """Create a DataFrame by reading from a temp CSV so _metadata is available."""
    csv_path = str(tmp_path / "test_data.csv")
    with open(csv_path, "w") as f:
        f.write("col1\n")
        f.write("val1\n")
        f.write("val2\n")
    return spark_session.read.format("csv").option("header", "true").load(csv_path)


def test_add_metadata_columns_include_time_true(spark_session, file_backed_df):
    result_df = shared_transforms.add_metadata_columns(file_backed_df, include_time=True)

    assert "last_updated_time" in result_df.columns
    assert "source_file" in result_df.columns
    assert "last_updated_date" not in result_df.columns
    assert result_df.count() == 2


def test_add_metadata_columns_include_time_false(spark_session, file_backed_df):
    result_df = shared_transforms.add_metadata_columns(file_backed_df, include_time=False)

    assert "last_updated_date" in result_df.columns
    assert "source_file" in result_df.columns
    assert "last_updated_time" not in result_df.columns
    assert result_df.count() == 2
