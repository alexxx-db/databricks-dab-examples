python3 setup.py bdist_wheel
databricks workspace mkdirs /Shared/code
databricks workspace import --overwrite --format "AUTO" --file dist/flights-advanced-0.0.1-py3-none-any.whl /Shared/code/flights-advanced-0.0.1-py3-none-any.whl
