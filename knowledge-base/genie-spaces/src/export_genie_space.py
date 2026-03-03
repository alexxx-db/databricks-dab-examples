# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Export Genie Space
# MAGIC
# MAGIC Utility notebook to export an existing Genie Space configuration for version control.
# MAGIC Fetches the space definition and reverse-substitutes catalog/schema values with
# MAGIC `{{catalog}}`/`{{schema}}` placeholders so the output can be saved to `src/spaces/`.

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Parameters
dbutils.widgets.text("space_id", "", "Genie Space ID to export")
dbutils.widgets.text("catalog", "", "Catalog to replace with {{catalog}}")
dbutils.widgets.text("schema", "", "Schema to replace with {{schema}}")

space_id = dbutils.widgets.get("space_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

assert space_id, "space_id parameter is required"
assert catalog, "catalog parameter is required"
assert schema, "schema parameter is required"

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

def reverse_substitute(text, catalog, schema):
    """Replace concrete catalog.schema references with placeholders."""
    if isinstance(text, str):
        text = text.replace(f"{catalog}.{schema}", "{{catalog}}.{{schema}}")
        text = text.replace(catalog, "{{catalog}}")
        text = text.replace(schema, "{{schema}}")
    return text


def reverse_substitute_recursive(obj, catalog, schema):
    """Recursively reverse-substitute placeholders in a nested structure."""
    if isinstance(obj, str):
        return reverse_substitute(obj, catalog, schema)
    elif isinstance(obj, dict):
        return {k: reverse_substitute_recursive(v, catalog, schema) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [reverse_substitute_recursive(item, catalog, schema) for item in obj]
    return obj

# COMMAND ----------

print(f"Exporting Genie Space: {space_id}")
space = w.genie.get_space(space_id)

# Build the exportable config
config = {
    "title": space.title,
    "description": space.description or "",
    "warehouse_id": "{{warehouse_id}}",
}

# Table identifiers
if space.table_identifiers:
    config["table_identifiers"] = [
        {"table_identifier": t.table_identifier, "id": t.id}
        for t in space.table_identifiers
    ]

# Sample questions
if space.sample_questions:
    config["sample_questions"] = list(space.sample_questions)

# Instructions
if space.instructions:
    config["instructions"] = list(space.instructions)

# Example SQLs
if space.example_sqls:
    config["example_sqls"] = [
        {"question": eq.question, "sql": eq.sql}
        for eq in space.example_sqls
    ]

# Table joins
if space.table_joins:
    config["table_joins"] = [
        {
            "left_table_id": tj.left_table_id,
            "right_table_id": tj.right_table_id,
            "left_column": tj.left_column,
            "right_column": tj.right_column,
        }
        for tj in space.table_joins
    ]

# Reverse-substitute catalog/schema values with placeholders
config = reverse_substitute_recursive(config, catalog, schema)

# COMMAND ----------

# Output the JSON for copying to src/spaces/
output = json.dumps(config, indent=2)
print(output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Copy the JSON output above and save it as a `.json` file in `src/spaces/`.
# MAGIC For example: `src/spaces/my_space.json`
# MAGIC
# MAGIC Then remove any target suffix from the title (e.g., "(dev)") since the deploy
# MAGIC script adds it automatically.
