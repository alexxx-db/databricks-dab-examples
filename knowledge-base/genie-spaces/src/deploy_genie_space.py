# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy Genie Spaces
# MAGIC
# MAGIC This notebook deploys Genie Spaces defined in `src/spaces/*.json` using the
# MAGIC [Genie Management API](https://docs.databricks.com/api/workspace/genie). It creates
# MAGIC new spaces or updates existing ones, supporting environment-aware deployments.

# COMMAND ----------

import json
import os
import re
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import (
    GenieSpace,
    GenieTableIdentifier,
    GenieExampleQuery,
    GenieTableJoin,
)

# COMMAND ----------

# Parameters from the DAB job
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("bundle_target", "dev")
dbutils.widgets.text("parent_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")
bundle_target = dbutils.widgets.get("bundle_target")
parent_path = dbutils.widgets.get("parent_path")

print(f"Deploying Genie Spaces for target: {bundle_target}")
print(f"Catalog: {catalog}, Schema: {schema}")
print(f"Warehouse ID: {warehouse_id}")

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def resolve_placeholders(text, catalog, schema, warehouse_id):
    """Replace {{catalog}}, {{schema}}, and {{warehouse_id}} placeholders in text."""
    if isinstance(text, str):
        text = text.replace("{{catalog}}", catalog)
        text = text.replace("{{schema}}", schema)
        text = text.replace("{{warehouse_id}}", warehouse_id)
    return text


def resolve_placeholders_recursive(obj, catalog, schema, warehouse_id):
    """Recursively resolve placeholders in a nested structure."""
    if isinstance(obj, str):
        return resolve_placeholders(obj, catalog, schema, warehouse_id)
    elif isinstance(obj, dict):
        return {k: resolve_placeholders_recursive(v, catalog, schema, warehouse_id) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_placeholders_recursive(item, catalog, schema, warehouse_id) for item in obj]
    return obj

# COMMAND ----------

def get_notebook_dir():
    """Get the directory of the current notebook."""
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return str(Path(notebook_path).parent)
    except Exception:
        return "/Workspace"


def load_space_config(config_path):
    """Load a Genie Space configuration from a JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)

# COMMAND ----------

def get_state_file_path():
    """Get the path to the state file for the current target."""
    notebook_dir = get_notebook_dir()
    return os.path.join("/Workspace", notebook_dir.lstrip("/"), "space_state.json")


def load_state(state_path):
    """Load deployment state from file."""
    try:
        with open(state_path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state_path, state):
    """Save deployment state to file."""
    with open(state_path, "w") as f:
        json.dump(state, f, indent=2)

# COMMAND ----------

def find_space_by_title(title):
    """Search for an existing Genie Space by title. Returns space_id or None."""
    try:
        spaces = w.genie.list_spaces()
        for space in spaces:
            if space.title == title:
                return space.space_id
    except Exception as e:
        print(f"  Warning: Could not list spaces: {e}")
    return None

# COMMAND ----------

def build_serialized_space(config, catalog, schema, warehouse_id, target):
    """Build a GenieSpace object from a resolved config dict."""
    resolved = resolve_placeholders_recursive(config, catalog, schema, warehouse_id)

    title = resolved["title"]
    if target:
        title = f"{title} ({target})"

    table_identifiers = []
    for t in resolved.get("table_identifiers", []):
        table_identifiers.append(
            GenieTableIdentifier(
                table_identifier=t["table_identifier"],
                id=t.get("id"),
            )
        )

    example_sqls = []
    for eq in resolved.get("example_sqls", []):
        example_sqls.append(
            GenieExampleQuery(
                question=eq["question"],
                sql=eq["sql"],
            )
        )

    table_joins = []
    for tj in resolved.get("table_joins", []):
        table_joins.append(
            GenieTableJoin(
                left_table_id=tj["left_table_id"],
                right_table_id=tj["right_table_id"],
                left_column=tj["left_column"],
                right_column=tj["right_column"],
            )
        )

    return GenieSpace(
        title=title,
        description=resolved.get("description", ""),
        warehouse_id=resolved.get("warehouse_id", warehouse_id),
        table_identifiers=table_identifiers,
        sample_questions=resolved.get("sample_questions"),
        instructions=resolved.get("instructions"),
        example_sqls=example_sqls if example_sqls else None,
        table_joins=table_joins if table_joins else None,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Spaces

# COMMAND ----------

def discover_space_configs():
    """Discover all .json files in the spaces directory."""
    notebook_dir = get_notebook_dir()
    spaces_dir = os.path.join("/Workspace", notebook_dir.lstrip("/"), "spaces")
    configs = []
    if os.path.isdir(spaces_dir):
        for filename in sorted(os.listdir(spaces_dir)):
            if filename.endswith(".json"):
                configs.append(os.path.join(spaces_dir, filename))
    return configs

# COMMAND ----------

# Main deployment logic
state_path = get_state_file_path()
state = load_state(state_path)
target_state = state.get(bundle_target, {})
results = []

config_files = discover_space_configs()
print(f"Found {len(config_files)} space configuration(s)")

for config_path in config_files:
    config_name = Path(config_path).stem
    print(f"\nProcessing: {config_name}")

    config = load_space_config(config_path)
    space_obj = build_serialized_space(config, catalog, schema, warehouse_id, bundle_target)

    space_id = target_state.get(config_name)
    action = None

    # Check if the space still exists
    if space_id:
        try:
            existing = w.genie.get_space(space_id)
            print(f"  Found existing space: {space_id}")
        except Exception:
            print(f"  Space {space_id} no longer exists, will recreate")
            space_id = None

    # Fallback: search by title
    if not space_id:
        space_id = find_space_by_title(space_obj.title)
        if space_id:
            print(f"  Found space by title: {space_id}")

    # Create or update
    if space_id:
        print(f"  Updating space {space_id}...")
        updated = w.genie.update_space(
            space_id=space_id,
            title=space_obj.title,
            description=space_obj.description,
            warehouse_id=space_obj.warehouse_id,
            table_identifiers=space_obj.table_identifiers,
            sample_questions=space_obj.sample_questions,
            instructions=space_obj.instructions,
            example_sqls=space_obj.example_sqls,
            table_joins=space_obj.table_joins,
        )
        space_id = updated.space_id
        action = "updated"
    else:
        print(f"  Creating new space...")
        created = w.genie.create_space(
            title=space_obj.title,
            description=space_obj.description,
            warehouse_id=space_obj.warehouse_id,
            table_identifiers=space_obj.table_identifiers,
            sample_questions=space_obj.sample_questions,
            instructions=space_obj.instructions,
            example_sqls=space_obj.example_sqls,
            table_joins=space_obj.table_joins,
        )
        space_id = created.space_id
        action = "created"

    print(f"  Space {action}: {space_id}")
    target_state[config_name] = space_id
    results.append({
        "config_name": config_name,
        "space_id": space_id,
        "action": action,
        "title": space_obj.title,
    })

# Save state
state[bundle_target] = target_state
save_state(state_path, state)
print(f"\nState saved to {state_path}")

# COMMAND ----------

# Pass results to the validation task
dbutils.jobs.taskValues.set(key="deploy_results", value=json.dumps(results))
dbutils.jobs.taskValues.set(key="catalog", value=catalog)
dbutils.jobs.taskValues.set(key="schema", value=schema)
dbutils.jobs.taskValues.set(key="bundle_target", value=bundle_target)

print("\nDeployment Summary:")
for r in results:
    print(f"  {r['config_name']}: {r['action']} -> {r['space_id']}")
