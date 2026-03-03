# Genie Spaces - DAB Deployment Example

This project demonstrates how to deploy [**Genie Spaces**](https://docs.databricks.com/aws/en/genie/index.html) using [**Databricks Asset Bundles**](https://docs.databricks.com/aws/en/dev-tools/bundles/) and the [Genie Management API](https://docs.databricks.com/api/workspace/genie) (Public Preview).

Since DABs do not natively support Genie Spaces as a resource type ([databricks/cli #3008](https://github.com/databricks/cli/issues/3008)), this example uses a DAB-managed job that calls the Genie API to create or update spaces programmatically. This enables version-controlled, environment-aware Genie Space management.

## Features

- **Declarative space definitions**: Define Genie Spaces as JSON files with placeholder tokens for catalog, schema, and warehouse
- **Environment-aware deployments**: Spaces are suffixed with the target name (e.g., "Sales Analytics (dev)")
- **Idempotent upserts**: Detects existing spaces via state file or title search, creates or updates accordingly
- **Export utility**: Export existing Genie Spaces back to JSON for version control
- **Post-deploy validation**: Automated checks that spaces exist and are correctly configured

## Prerequisites

### 1. Install the Databricks CLI
Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/install.html

### 2. Authenticate to your Databricks workspace
```bash
databricks configure --token --profile DEFAULT
```

### 3. Configure databricks.yml Variables
Update the variables in `databricks.yml` to match your environment:

- **catalog**: The catalog containing your tables
- **schema**: The schema containing your tables
- **warehouse_id**: ID of your SQL warehouse
- **workspace.host**: Your Databricks workspace URL

## Project Structure

```
genie-spaces/
├── databricks.yml              # Bundle configuration
├── README.md
├── .gitignore
├── resources/
│   └── genie_deploy_job.yml    # Job definition with deploy + validate tasks
├── src/
│   ├── deploy_genie_space.py   # Main deployment notebook
│   ├── export_genie_space.py   # Utility to export existing spaces
│   ├── space_state.json        # Tracks deployed space IDs per target
│   └── spaces/
│       └── sales_analytics.json # Sample space definition
└── tests/
    └── validate_genie_space.py # Post-deploy validation
```

## Deployment

### Deploy to Development
```bash
databricks bundle deploy --target dev
databricks bundle run genie_spaces_deploy --target dev
```

### Deploy to Production
```bash
databricks bundle deploy --target prod --profile PROD
databricks bundle run genie_spaces_deploy --target prod --profile PROD
```

## Adding New Genie Spaces

1. Create a new JSON file in `src/spaces/` (e.g., `src/spaces/my_space.json`)
2. Use `{{catalog}}`, `{{schema}}`, and `{{warehouse_id}}` placeholders for environment-specific values
3. Include table identifiers with 32-character hex IDs, sample questions, instructions, and optionally example SQLs and table joins
4. Deploy the bundle - the deploy job will automatically discover and deploy all `.json` files in `src/spaces/`

## Exporting Existing Spaces

To export an existing Genie Space for version control:

1. Open the `src/export_genie_space.py` notebook in your workspace
2. Set the parameters: `space_id`, `catalog`, `schema`
3. Run the notebook - it outputs JSON with catalog/schema values replaced by placeholders
4. Copy the output to a new file in `src/spaces/`

## How It Works

1. **Discovery**: The deploy notebook scans `src/spaces/` for `.json` config files
2. **Placeholder resolution**: `{{catalog}}`, `{{schema}}`, and `{{warehouse_id}}` are replaced with target-specific values
3. **Upsert logic**: For each space config:
   - Check the state file for a previously deployed space ID
   - Verify the space still exists via API
   - Fall back to searching by title
   - Create a new space or update the existing one
4. **State tracking**: Space IDs are persisted in `space_state.json` per target
5. **Validation**: The validation task confirms spaces exist and are correctly configured

## Documentation

- [Genie Spaces](https://docs.databricks.com/aws/en/genie/index.html)
- [Genie Management API](https://docs.databricks.com/api/workspace/genie)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
