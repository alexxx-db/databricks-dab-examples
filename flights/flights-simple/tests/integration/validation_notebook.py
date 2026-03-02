# Databricks notebook source
# MAGIC %md
# MAGIC # Validation notebook
# MAGIC
# MAGIC This notebook is executed using Databricks Workflows as defined in resources/notebook_validation_job.yml. It is used to check summary table for valid results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Frame assert
# MAGIC Compare results from test data set against an expected set of values that is generated with simpler logic. This is more dynamic but involves putting more logic into the test.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("database", "flights_dev")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")

# COMMAND ----------

from pyspark.testing.utils import assertDataFrameEqual
from flights.utils import flight_utils

result_counts = spark.sql(f"""
        SELECT sum(case when deptime is null then 1 else 0 end) null_deptime_count, count(1) rows
        FROM {catalog}.{database}.flights_raw
        """)

csv_schema = flight_utils.get_flight_schema()

expected_df = (spark.read.format("csv")
      .option("header", "false")
      .schema(csv_schema)
      .load("/databricks-datasets/airlines/")
      .limit(1000)
    )
expected_df.createOrReplaceTempView("expected_flights_raw")
expected_counts = spark.sql("""
        SELECT sum(case when deptime is null then 1 else 0 end) null_deptime_count, count(1) rows
        FROM expected_flights_raw
        """)

assertDataFrameEqual(result_counts, expected_counts)

# COMMAND ----------

result_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple assert
# MAGIC Option you can use if counts will stay consistent in the test environment.

# COMMAND ----------

from pyspark.sql import Row

result = spark.sql(f"""
        SELECT count(distinct deptime) deptime_count, count(1) rows
        FROM {catalog}.{database}.flights_raw
        """).first()

# Option 1
# assert result.dt_count == 398
assert result.rows == 1000


# COMMAND ----------

print("No errors detected")

# COMMAND ----------


