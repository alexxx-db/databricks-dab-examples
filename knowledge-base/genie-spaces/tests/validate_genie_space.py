# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate Genie Spaces
# MAGIC
# MAGIC Post-deployment validation notebook. Checks that all deployed Genie Spaces
# MAGIC exist and have the expected configuration.

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

# Read deployment results from the deploy task
try:
    deploy_results_raw = dbutils.jobs.taskValues.get(
        taskKey="deploy_genie_spaces", key="deploy_results"
    )
    deploy_results = json.loads(deploy_results_raw)
    catalog = dbutils.jobs.taskValues.get(taskKey="deploy_genie_spaces", key="catalog")
    schema = dbutils.jobs.taskValues.get(taskKey="deploy_genie_spaces", key="schema")
    bundle_target = dbutils.jobs.taskValues.get(
        taskKey="deploy_genie_spaces", key="bundle_target"
    )
    print(f"Loaded {len(deploy_results)} result(s) from deploy task")
except Exception as e:
    print(f"Could not read task values ({e}), falling back to state file")
    import os
    from pathlib import Path

    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("bundle_target", "dev")

    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    bundle_target = dbutils.widgets.get("bundle_target")

    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    # State file is in ../src/ relative to tests/
    src_dir = str(Path(notebook_path).parent.parent / "src")
    if not src_dir.startswith("/Workspace"):
        src_dir = "/Workspace" + src_dir
    state_path = os.path.join(src_dir, "space_state.json")

    with open(state_path, "r") as f:
        state = json.load(f)

    target_state = state.get(bundle_target, {})
    deploy_results = [
        {"config_name": name, "space_id": sid, "action": "from_state", "title": ""}
        for name, sid in target_state.items()
    ]
    print(f"Loaded {len(deploy_results)} result(s) from state file")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Checks

# COMMAND ----------

errors = []
validated = 0

for result in deploy_results:
    space_id = result["space_id"]
    config_name = result["config_name"]
    print(f"\nValidating: {config_name} ({space_id})")

    # 1. Space exists and is retrievable
    try:
        space = w.genie.get_space(space_id)
        print(f"  [PASS] Space exists: {space.title}")
    except Exception as e:
        errors.append(f"{config_name}: Space {space_id} not found - {e}")
        print(f"  [FAIL] Space not found: {e}")
        continue

    # 2. Title contains the target name
    if bundle_target and bundle_target in space.title:
        print(f"  [PASS] Title contains target '{bundle_target}'")
    else:
        errors.append(
            f"{config_name}: Title '{space.title}' does not contain target '{bundle_target}'"
        )
        print(f"  [FAIL] Title missing target '{bundle_target}'")

    # 3. Serialized space structure is valid and table identifiers use correct catalog/schema
    serialized = json.loads(space.serialized_space) if space.serialized_space else {}
    tables = serialized.get("data_sources", {}).get("tables", [])
    if tables:
        print(f"  [PASS] Has {len(tables)} table identifier(s)")
        for t in tables:
            ti = t.get("table_identifier", "")
            if ti.startswith(f"{catalog}.{schema}."):
                print(f"    [PASS] Table '{ti}' has correct catalog.schema")
            else:
                errors.append(
                    f"{config_name}: Table '{ti}' does not match {catalog}.{schema}"
                )
                print(f"    [FAIL] Table '{ti}' has wrong catalog.schema")
    else:
        errors.append(f"{config_name}: No table identifiers found in serialized_space")
        print(f"  [FAIL] No table identifiers")

    # 4. Warehouse ID is set
    if space.warehouse_id:
        print(f"  [PASS] Warehouse ID is set: {space.warehouse_id}")
    else:
        errors.append(f"{config_name}: No warehouse ID set")
        print(f"  [FAIL] No warehouse ID")

    validated += 1

# COMMAND ----------

# Summary
print(f"\n{'=' * 60}")
print(f"Validation complete: {validated}/{len(deploy_results)} spaces validated")

if errors:
    print(f"\n{len(errors)} error(s) found:")
    for err in errors:
        print(f"  - {err}")
    raise AssertionError(
        f"Genie Space validation failed with {len(errors)} error(s): {'; '.join(errors)}"
    )
else:
    print("All validations passed!")
