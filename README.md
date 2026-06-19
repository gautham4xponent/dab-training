# Databricks Asset Bundle Walkthrough

This repository is a Databricks Asset Bundle (DAB) walkthrough. The NYC taxi workload is only the sample domain; the real learning path is how a bundle packages code, declares Databricks resources, validates changes, and deploys consistently across dev, staging, and prod.

## 1. Local Setup

Install the command-line tools used by the project:

```powershell
winget install Databricks.DatabricksCLI
winget install Microsoft.AzureCLI
```

Create a Python 3.11 virtual environment. Python 3.11 is used because PySpark 3.5.x works cleanly with it locally.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
```

`requirements.txt` intentionally stays small:

```text
pyspark==3.5.5
pytest==9.0.3
```

PySpark installs `py4j`; pytest installs its own test dependencies. The editable install makes the local `dab_training` package importable for unit tests.

## 2. Authenticate For Bundle Work

For normal Databricks operations, authenticate the Databricks CLI profile:

```powershell
databricks auth login --profile dab-training
```

For Azure Key Vault-backed secret scopes, the deploy needs an Azure user AAD token. Use Azure CLI auth in the same terminal session:

```powershell
az login
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
```

Without Azure CLI auth, Key Vault scope creation can fail with:

```text
Scope with AzureKeyVault must have userAADToken defined
```

For service principal deploys in CI/CD, this project uses `azure-client-secret` instead.

## 3. Bundle Root

`databricks.yml` is the bundle entry point, separate from the resource files. It defines:

- Bundle identity: `bundle.name` and `bundle.uuid`.
- File includes: `resources/*.yml`.
- Shared variables: catalog, cluster size, warehouse size, warehouse ID.
- Workspace root path: where bundle files and state are uploaded.
- Wheel artifact build command.
- Deployment targets: `dev`, `staging`, and `prod`.

The target controls which workspace and catalog are used:

```yaml
targets:
  dev:
    variables:
      catalog_name: xponent_dev
  staging:
    mode: production
    variables:
      catalog_name: xponent_staging
```

The `dev` target currently has `mode: development` commented out. That avoids automatic dev-name prefixing that can conflict with code paths that still expect the schema name `dab_training`.

## 4. Packaging The Wheel

The wheel journey starts in `databricks.yml`:

```yaml
artifacts:
  dab_training:
    type: whl
    build: "python setup.py bdist_wheel"
    path: .
```

That command runs `setup.py`. `setup.py` uses `find_packages(where="src")`, so it packages Python modules under `src/`. The empty `src/dab_training/__init__.py` marks `dab_training` as a package.

The important entry point is:

```python
"main=dab_training.transform:main"
```

During deployment, the bundle builds `dist/dab_training-0.1.0-py3-none-any.whl`, uploads it to the bundle artifact path, and the Databricks job installs that wheel into the serverless job environment.

At runtime, the job calls:

```yaml
python_wheel_task:
  package_name: dab_training
  entry_point: main
```

That resolves to `src/dab_training/transform.py::main`.

## 5. Job Orchestration

`resources/jobs.yml` defines the main multi-task job. It is the orchestration layer for the walkthrough.

The job has three tasks:

- `ingest`: notebook task that loads sample taxi data and writes `taxi_raw`.
- `transform`: wheel task that reads `taxi_raw`, cleans it, and writes `taxi_transformed`.
- `dlt_pipeline`: pipeline task that runs the DLT/Lakeflow pipeline after transform.

The job declares a serverless environment:

```yaml
environments:
  - environment_key: default_serverless_env
    spec:
      environment_version: "2"
      dependencies:
        - ${workspace.root_path}/artifacts/.internal/dab_training-0.1.0-py3-none-any.whl
```

That environment is attached to the wheel task. The notebook task and pipeline task use Databricks-managed serverless execution rather than a manually declared all-purpose cluster.

## 6. Ingest Notebook

`src/01_ingest.py` is deployed as a notebook because `jobs.yml` references it with:

```yaml
notebook_path: ../src/01_ingest.py
```

The notebook receives `catalog_name` from the job:

```yaml
base_parameters:
  catalog_name: ${var.catalog_name}
```

It then:

- Reads Databricks sample NYC taxi data.
- Creates the target schema if needed.
- Writes `taxi_raw` to Unity Catalog.
- Sets a task value named `row_count`.
- Writes a small parquet sample to `/Volumes/<catalog>/dab_training/raw_files/`.

The task value is later read by the wheel task, demonstrating task-to-task communication.

## 7. Transform Wheel

`src/dab_training/transform.py` is the packaged Python code used by the wheel task.

The `main()` function:

- Reads the catalog argument passed by the job.
- Creates a Spark session.
- Reads the `row_count` task value from the upstream ingest task.
- Calls `transform(spark, catalog)`.

The `transform()` function:

- Reads `<catalog>.dab_training.taxi_raw`.
- Calculates `trip_duration_min`.
- Filters bad durations and non-positive fares.
- Writes `<catalog>.dab_training.taxi_transformed`.

The timestamp conversion is deliberate:

```python
F.to_timestamp("dropoff_datetime").cast("long")
```

Casting timestamp-like strings directly to `long` produced `NULL` in local Spark tests, so the code converts to timestamp first.

## 8. DLT/Lakeflow Pipeline

`resources/pipelines.yml` defines the serverless pipeline:

```yaml
catalog: ${var.catalog_name}
schema: dab_training
serverless: true
```

It deploys `src/02_dlt_pipeline.py` as the pipeline library and passes `catalog_name` through configuration.

The pipeline code builds a small medallion flow:

- `taxi_bronze`: streaming read from `taxi_transformed`.
- `taxi_silver`: selected and quality-checked trip records.
- `taxi_gold_daily`: daily aggregate metrics used by SQL assets.

The job does not hardcode a pipeline ID. It references the pipeline resource:

```yaml
pipeline_id: ${resources.pipelines.taxi_dlt_pipeline.id}
```

That lets DAB create or update the pipeline and wire the job to the correct deployed pipeline.

## 9. Catalog And Volume Resources

`resources/catalog.yml` declares Unity Catalog resources.

The schema resource creates:

```text
<catalog_name>.dab_training
```

where `catalog_name` changes by target, for example `xponent_dev` or `xponent_staging`.

The volume resource creates:

```text
/Volumes/<catalog_name>/dab_training/raw_files/
```

It references the schema resource directly:

```yaml
schema_name: ${resources.schemas.training_schema.name}
```

That creates an explicit dependency: DAB knows the schema must exist before the volume.

The grants in this file demonstrate Unity Catalog permission management through the bundle. Service principals are referenced by application/client ID to avoid display-name resolution issues.

## 10. SQL Alert And Dashboard

`resources/sql_assets.yml` demonstrates Databricks SQL resources.

The alert `low_trip_count_alert` runs an inline query against the gold table:

```sql
FROM ${var.catalog_name}.dab_training.taxi_gold_daily
```

It uses `${var.warehouse_id}` because this workspace does not create SQL warehouses through the bundle.

The dashboard resource deploys `src/taxi_dashboard.lvdash.json`:

```yaml
dashboards:
  taxi_analytics_dashboard:
    file_path: ../src/taxi_dashboard.lvdash.json
    warehouse_id: ${var.warehouse_id}
    dataset_catalog: ${var.catalog_name}
    dataset_schema: dab_training
```

The dashboard JSON contains the saved Lakeview layout and dataset queries. The bundle resource binds that dashboard to the target catalog, schema, and warehouse.

## 11. Secret Scope

`resources/secret-scopes.yml` declares an Azure Key Vault-backed Databricks secret scope:

```yaml
backend_type: AZURE_KEYVAULT
keyvault_metadata:
  resource_id: ...
  dns_name: ...
```

This resource is different from pure Databricks resources because Databricks must validate and bind to Azure Key Vault. For local user-to-machine deploys, use:

```powershell
az login
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
databricks bundle deploy --target dev --profile dab-training
```

For machine-to-machine deployment, the deploying service principal and the Databricks-managed Azure identity may also need Azure permissions on the Key Vault. If permissions fail, the error usually comes from Azure IAM or Key Vault access policies rather than YAML syntax.

## 12. Tests

`tests/test_transform.py` validates the local PySpark transform behavior.

The file sets:

```python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

That forces Spark worker processes to use the same Python executable as the active venv. Without this, local Spark can accidentally use a different global Python and crash or behave inconsistently.

Run tests locally:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/ -v
```

## 13. Local Deployment Path

Validate first:

```powershell
databricks bundle validate --target dev --profile dab-training
```

Deploy dev:

```powershell
az login
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
databricks bundle deploy --target dev --profile dab-training
```

Deploy staging:

```powershell
az login
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
databricks bundle deploy --target staging --profile dab-training-stg
```

Run the deployed job:

```powershell
databricks bundle run dab_training_job --target dev --profile dab-training
```

Destroy dev resources managed by the bundle:

```powershell
databricks bundle destroy --target dev --profile dab-training
```

## 14. CI Pipeline

`.github/workflows/ci.yml` validates pull requests and manual runs.

The job is named `Bundle Validate & Unit Tests` and does the following:

1. Checks out the repository.
2. Sets up Python 3.11.
3. Installs the Databricks CLI.
4. Installs test dependencies and the local package.
5. Builds the wheel with `python setup.py bdist_wheel`.
6. Runs `databricks bundle validate --target dev`.
7. Runs `pytest tests/ -v --tb=short --junit-xml=test-results.xml`.
8. Uploads the test result XML as an artifact even if tests fail.

The validate step uses service principal auth:

```yaml
DATABRICKS_AUTH_TYPE: azure-client-secret
DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
ARM_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
ARM_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
ARM_TENANT_ID: ${{ secrets.DATABRICKS_AZURE_TENANT_ID }}
```

The goal of CI is not to deploy. It confirms the bundle can resolve and that the Python logic passes local tests.

## 15. CD Pipeline

`.github/workflows/cd.yml` deploys after changes merge to `main`, and can also be triggered manually.

The active staging job does the following:

1. Checks out the repository.
2. Installs the Databricks CLI.
3. Runs `databricks current-user me` to confirm which identity is deploying.
4. Builds the wheel artifact with `python setup.py bdist_wheel`.
5. Runs `databricks bundle deploy --target staging`.

The workflow-level environment uses service principal auth:

```yaml
DATABRICKS_AUTH_TYPE: azure-client-secret
DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
ARM_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
ARM_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
ARM_TENANT_ID: ${{ secrets.DATABRICKS_AZURE_TENANT_ID }}
```

The production job is present as a commented template. It is intended to run after staging succeeds and after an approval gate is added through GitHub Environments.

## 16. Important Lessons From This Bundle

- DAB deploys Databricks resources; Azure resources such as Key Vault must already exist and be permissioned.
- Key Vault-backed secret scopes need Azure-aware authentication, not PAT auth.
- Wheel deployment depends on `setup.py`, the `src/dab_training` package, and the job environment dependency.
- Bundle resource references, such as `${resources.pipelines.taxi_dlt_pipeline.id}`, keep resources wired together without hardcoded IDs.
- If `mode: development` is enabled, resource names may be prefixed. Any hardcoded schema references like `dab_training` should be parameterized first.
