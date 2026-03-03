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

# Parse the serialized_space JSON containing all space configuration
serialized = json.loads(space.serialized_space) if space.serialized_space else {}

# Build the exportable config in our user-friendly format
config = {
    "title": space.title,
    "description": space.description or "",
    "warehouse_id": "{{warehouse_id}}",
}

# Table identifiers from serialized_space
tables = serialized.get("data_sources", {}).get("tables", [])
if tables:
    config["table_identifiers"] = tables

# Sample questions
sample_qs = serialized.get("config", {}).get("sample_questions", [])
if sample_qs:
    config["sample_questions"] = sample_qs

# Instructions block
instr_block = serialized.get("instructions", {})

text_instr = instr_block.get("text_instructions", "")
if text_instr:
    # Split back into a list for readability
    config["instructions"] = [
        line.strip() for line in text_instr.split("\n") if line.strip()
    ]

example_sqls = instr_block.get("example_question_sqls", [])
if example_sqls:
    config["example_sqls"] = example_sqls

join_specs = instr_block.get("join_specs", [])
if join_specs:
    config["table_joins"] = join_specs

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
