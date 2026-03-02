import os
import pytest

from databricks.sdk import WorkspaceClient


@pytest.fixture
def ws_conn():
    host = os.environ.get('DATABRICKS_HOST')
    client_id = os.environ.get('DATABRICKS_CLIENT_ID')
    client_secret = os.environ.get('DATABRICKS_CLIENT_SECRET')
    if not all([host, client_id, client_secret]):
        pytest.skip("DATABRICKS_HOST, DATABRICKS_CLIENT_ID, and DATABRICKS_CLIENT_SECRET must be set")
    return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)


def test_unity_catalog_objects(ws_conn):
    catalogs = list(ws_conn.catalogs.list())
    assert len(catalogs) > 0, "Expected at least one catalog"

    first_catalog = catalogs[0]
    schemas = list(ws_conn.schemas.list(catalog_name=first_catalog.name))
    assert len(schemas) > 0, f"Expected at least one schema in catalog '{first_catalog.name}'"
