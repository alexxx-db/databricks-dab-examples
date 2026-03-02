import pytest
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual
from pyspark.sql.types import IntegerType, StringType
import sys

sys.path.append('./src')

from flights.utils import flight_utils

def test_get_flight_schema__valid():
    schema = flight_utils.get_flight_schema()
    assert schema is not None
    assert len(schema) == 31

def test_get_flight_schema__field_names():
    schema = flight_utils.get_flight_schema()
    field_names = [f.name for f in schema.fields]
    assert field_names[0] == "Year"
    assert field_names[1] == "Month"
    assert "UniqueCarrier" in field_names
    assert "DepTime" in field_names
    assert "IsArrDelayed" in field_names
    assert "IsDepDelayed" in field_names

def test_get_flight_schema__field_types():
    schema = flight_utils.get_flight_schema()
    fields_by_name = {f.name: f for f in schema.fields}
    assert fields_by_name["Year"].dataType == IntegerType()
    assert fields_by_name["Month"].dataType == IntegerType()
    assert fields_by_name["UniqueCarrier"].dataType == StringType()
    assert fields_by_name["DepTime"].dataType == StringType()
    assert fields_by_name["FlightNum"].dataType == IntegerType()
    assert fields_by_name["Cancelled"].dataType == IntegerType()
