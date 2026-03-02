import argparse


def get_args():
    parser = argparse.ArgumentParser(description="Setup Unity Catalog objects for flights project")
    parser.add_argument("--catalog", type=str, default="main", help="Target catalog")
    parser.add_argument("--flights-schema", type=str, default="flights_dev", help="Flights schema name")
    parser.add_argument("--flights-validation-schema", type=str, default="flights_validation_dev", help="Flights validation schema name")
    return parser.parse_args()


args = get_args()
catalog = args.catalog
flights_schema = args.flights_schema
flights_validation_schema = args.flights_validation_schema

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ModuleNotFoundError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{flights_schema};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{flights_validation_schema};")
