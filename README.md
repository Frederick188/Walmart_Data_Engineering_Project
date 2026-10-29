# Walmart Data Engineering Project

An end-to-end data engineering project combining Neon PostgreSQL, Amazon
S3, Databricks, dbt, and Apache Airflow to demonstrate ingestion,
lakehouse transformation, data quality, and workflow orchestration for
retail data.

> **Architecture note:** This README uses the architecture diagram
> supplied for reference. The diagram depicts a PostgreSQL source
> database, CDC and file-based ingestion paths, incremental processing,
> a One-Big-Table stage, quality checks, dbt transformations, and
> Airflow orchestration. Confirm which of these are implemented in the
> current repository before describing them as production-ready. Replace
> placeholders with your actual configuration; never commit secrets.

## Table of Contents

-   [Project Overview](#project-overview)
-   [Architecture](#architecture)
-   [End-to-End Data Flow](#end-to-end-data-flow)
-   [Medallion Architecture](#medallion-architecture)
-   [Technology Stack and
    Responsibilities](#technology-stack-and-responsibilities)
-   [Neon PostgreSQL](#neon-postgresql)
-   [Amazon S3](#amazon-s3)
-   [Databricks and Unity Catalog](#databricks-and-unity-catalog)
-   [dbt](#dbt)
-   [Apache Airflow](#apache-airflow)
-   [Repository Structure](#repository-structure)
-   [Configuration and Secrets](#configuration-and-secrets)
-   [Running and Validating the
    Project](#running-and-validating-the-project)
-   [Data Quality and Reliability](#data-quality-and-reliability)
-   [Security](#security)
-   [Possible Extensions](#possible-extensions)

## Project Overview

The project demonstrates a modern data engineering workflow for retail
data. Neon PostgreSQL acts as the relational source, Amazon S3 provides
object storage for data files, Databricks provides lakehouse compute and
data access, dbt manages SQL transformation models and tests, and Apache
Airflow coordinates scheduled execution.

The supplied reference diagram illustrates these logical components:

-   A PostgreSQL source database.
-   A CDC path for change-based ingestion and a file path from S3.
-   Incremental processing.
-   A One-Big-Table (OBT) stage.
-   Data quality checks.
-   dbt-managed transformation logic.
-   Airflow orchestration.

The exact connectors and execution order depend on the implementation in
this repository. Treat CDC, OBT, quality checks, and automated
orchestration as implemented only where corresponding code or
configuration exists.

## Architecture

``` text
                  +--------------------------------+
                  |       Neon PostgreSQL          |
                  |    Relational source data      |
                  +---------------+----------------+
                                  |
                       CDC / extraction path
                                  |
                                  v
+--------------------+   +--------------------------+
|     Amazon S3      |-->|       Databricks         |
| Raw files / data   |   | Ingestion / incremental  |
| lake objects       |   | processing               |
+--------------------+   +-------------+------------+
                                       |
                                       v
                         +---------------------------+
                         | Bronze: source-aligned    |
                         | data                      |
                         +-------------+-------------+
                                       |
                                       v
                         +---------------------------+
                         | Silver: cleaned and       |
                         | conformed data            |
                         +-------------+-------------+
                                       |
                                       v
                         +---------------------------+
                         | Gold: OBT / fact and      |
                         | dimension models          |
                         +-------------+-------------+
                                       |
                                       v
                         +---------------------------+
                         | dbt transformations,      |
                         | tests and documentation   |
                         +-------------+-------------+
                                       ^
                                       |
                         +-------------+-------------+
                         | Apache Airflow            |
                         | Schedule and orchestrate  |
                         | configured jobs           |
                         +---------------------------+
```

This is a logical architecture, not a guarantee that each arrow is
implemented exactly as drawn. The supplied reference diagram places CDC
and S3 file ingestion paths ahead of incremental processing, then shows
an OBT stage, quality checks, dbt, and Airflow. Adjust the diagram if
the actual DAG and Databricks job use a different sequence.

## End-to-End Data Flow

1.  **Source:** Retail records originate in Neon PostgreSQL.
2.  **Ingestion:** The configured ingestion process extracts data. The
    reference architecture includes a CDC path and a file-based path;
    use the path or paths implemented in your code.
3.  **Object storage:** Amazon S3 stores data objects such as CSV, JSON,
    or Parquet files.
4.  **Databricks access:** Unity Catalog storage credentials and
    external locations can provide governed access to the relevant S3
    path.
5.  **Bronze processing:** Source-aligned records are retained with
    minimal transformation.
6.  **Silver processing:** Data is cleaned, standardized, deduplicated,
    and validated.
7.  **Gold processing:** Data is shaped into business-oriented models,
    potentially including an OBT and/or fact and dimension tables.
8.  **dbt:** SQL models, dependencies, tests, and documentation make
    transformations repeatable and maintainable.
9.  **Airflow:** The DAG schedules and coordinates the configured jobs.
    The shown project code uses the Databricks SDK to trigger a
    Databricks job.
10. **Validation:** Airflow task results, Databricks job runs, and
    data-quality checks help confirm execution and data correctness.

## Medallion Architecture

The Bronze, Silver, and Gold layers represent progressive data
refinement. The exact models and materializations should be verified
against the current dbt project and Databricks catalog.

### Bronze --- raw or source-aligned

**Purpose:** Keep data close to its original form so it can be traced
and reprocessed.

Typical responsibilities: - Ingest records with minimal
transformation. - Preserve source identifiers and ingestion metadata
when available. - Retain source-aligned values for troubleshooting and
replay. - Support downstream cleaning without repeatedly querying the
source system.

Possible retail entities include customers, stores, products, orders,
employees, and reviews, if present in the source.

### Silver --- cleaned and conformed

**Purpose:** Improve consistency and reliability for downstream use.

Typical responsibilities: - Standardize column names and data types. -
Handle nulls and invalid values according to documented rules. - Remove
or resolve duplicate records. - Normalize timestamps and categories. -
Apply dbt tests such as `not_null`, `unique`, `relationships`, and
`accepted_values` where appropriate.

Silver models should make cleaning rules explicit so that consumers can
understand how the data was prepared.

### Gold --- analytics-ready

**Purpose:** Present business-oriented datasets that are convenient for
reporting and analysis.

Typical responsibilities: - Join related entities. - Build fact and
dimension models. - Calculate business metrics and aggregates. -
Optionally create a **One-Big-Table (OBT)** for selected analytical use
cases. - Provide stable datasets for dashboards and queries.

The Databricks workspace previously showed tables named `dim_customers`,
`dim_employees`, `dim_orders`, `dim_products`, `dim_stores`,
`fact_orders`, and `reviews` in a Gold schema. Verify the current schema
and model definitions before treating these as the complete inventory. A
star schema around `fact_orders` is appropriate only if the table grain
and keys support the intended relationships.

**OBT versus star schema:** An OBT combines multiple related entities
into a wider table for simpler querying. A star schema separates a
central fact table from descriptive dimensions. They are alternative
modeling approaches for different needs; one does not automatically
replace the other.

## Technology Stack and Responsibilities

  -----------------------------------------------------------------------
  Component                           Role in the project
  ----------------------------------- -----------------------------------
  Neon PostgreSQL                     Managed relational source database

  Amazon S3                           Object storage for data lake files

  Databricks                          Data processing and lakehouse
                                      compute

  Unity Catalog                       Governance for data assets and
                                      cloud storage access

  dbt                                 SQL models, transformation
                                      dependencies, tests, and
                                      documentation

  Apache Airflow                      Scheduling, orchestration, retries,
                                      and task monitoring

  Python / Databricks SDK             Programmatic interaction with
                                      Databricks jobs

  Docker Compose                      Local services, if configured in
                                      the repository

  Git / GitHub                        Version control and project
                                      documentation
  -----------------------------------------------------------------------

## Neon PostgreSQL

Neon is a managed PostgreSQL service. It can host relational source
tables and relationships without requiring you to manage the database
server.

Responsibilities: - Store operational or sample retail source data. -
Provide a PostgreSQL-compatible endpoint for ingestion. - Act as the
source system for extraction or change capture.

Keep the Neon connection string in an environment variable or secret
store. A connection string may contain a username and password, so it
must not be committed to Git.

## Amazon S3

Amazon S3 is object storage, not a relational database. It stores
objects identified by keys, often organized using path-like prefixes.

Important concepts: - **Bucket:** the top-level container for objects. -
**Object key:** the name identifying a file/object within a bucket. -
**IAM role:** the AWS identity used to grant scoped access. - **Unity
Catalog storage credential:** the Unity Catalog object that represents
the cloud identity Databricks uses. - **External location:** a Unity
Catalog object linking a cloud storage path to a storage credential and
governing access.

In this architecture, S3 can hold raw or staged files and act as data
lake storage. Databricks access should be granted through the
appropriate IAM and Unity Catalog configuration. Keep S3 Block Public
Access enabled; public bucket access is not a solution to role or
permission errors.

## Databricks and Unity Catalog

Databricks provides compute for data processing and querying. Unity
Catalog manages governed data assets and access to cloud storage.

Relevant responsibilities: - Access the configured S3 path through a
storage credential and external location. - Organize data using catalogs
and schemas. - Run configured ingestion and transformation jobs. - Store
and query the resulting tables. - Support the Bronze, Silver, and Gold
modeling pattern.

A **storage credential** defines the cloud identity Databricks uses. An
**external location** associates a storage path (for example, an S3 URI)
with that credential and provides a governance boundary. Both the IAM
trust relationship and the relevant S3 permissions must be correct.

## dbt

dbt makes SQL transformation logic modular, testable, and
version-controlled.

Typical capabilities: - Define models in SQL. - Express model
dependencies with `ref()` and source dependencies with `source()`. -
Choose materializations such as views, tables, incremental models, or
ephemeral models. - Configure tests for data quality. - Document models
and expose lineage through model dependencies.

Common commands, run from the directory containing `dbt_project.yml`
with a valid profile and target:

``` bash
dbt debug
dbt deps
dbt parse
dbt run
dbt test
```

These commands are examples. Ensure the installed dbt adapter and
profile target match the actual execution platform, such as Databricks,
and the intended catalog and schema.

## Apache Airflow

Airflow coordinates when tasks run and how they depend on each other. It
complements dbt and Databricks rather than replacing them.

The project has used `airflow/dags/orchestrate.py`. The visible DAG code
imports `WorkspaceClient` from the Databricks SDK and invokes
`ws.jobs.run_now(...)` with a configured job ID.

Depending on the complete implementation, the DAG can: - Schedule
workflow runs. - Trigger a Databricks job. - Monitor task status and
expose task logs. - Apply retries to configured tasks.

Check the full DAG implementation before claiming that it waits for
Databricks job completion or executes additional stages. A successful
Airflow DAG run means its tasks succeeded according to their code;
verify Databricks job output and table contents separately.

## Repository Structure

Representative layout based on the workspace shown during development.
Update this section to match the actual files in the repository.

``` text
Walmart_Data_Engineering_Project/
├── airflow_dbt_project/
│   ├── dags/
│   │   ├── orchestrate.py
│   │   └── test.py
│   ├── config/
│   ├── logs/                 # generated files; normally not committed
│   ├── plugins/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── requirements.txt
├── walmart_data_engineering/
│   └── walmart_dataset/
│       ├── data/             # source CSV files
│       ├── ddl/              # SQL schema definitions
│       └── load_data.py      # source loading script
├── walmart_dbt_project/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── analyses/
│   ├── macros/
│   ├── seeds/
│   ├── snapshots/
│   ├── tests/
│   └── dbt_project.yml
└── README.md
```

This is a representative tree inferred from the project workspace
screenshots. Verify exact folder names and files before publishing; the
local project may contain additional configuration files.

## Configuration and Secrets

Use environment variables, Airflow connections, Databricks secret
scopes, or a managed secrets service. Do not hard-code credentials in
Python files.

Example variable names (illustrative; adapt them to what your code
reads):

``` dotenv
NEON_DATABASE_URL=postgresql://<user>:<password>@<host>/<database>
DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
DATABRICKS_TOKEN=<set-locally-do-not-commit>
DATABRICKS_JOB_ID=<your-job-id>
S3_BUCKET_NAME=<your-bucket-name>
```

Recommended practices: 1. Keep local credentials in `.env` or a secret
manager. 2. Add `.env` to `.gitignore`. 3. Commit placeholders only in
`.env.example`. 4. Never commit tokens, passwords, credential-bearing
database URLs, or AWS secrets. 5. If a secret appears in a commit, log,
screenshot, or chat, revoke it and generate a replacement. Removing it
from the latest file does not erase it from older commits.

## Running and Validating the Project

Exact commands depend on the project's configuration, installed
versions, and Databricks job setup.

### 1. Configure the source and cloud access

-   Configure the Neon PostgreSQL source and its tables.
-   Configure the S3 bucket and the required IAM permissions.
-   Configure the Databricks storage credential and external location in
    Unity Catalog.
-   Confirm that Databricks can read and write the intended S3 path as
    required.

### 2. Validate and run dbt

From the directory containing `dbt_project.yml`:

``` bash
dbt debug
dbt deps
dbt parse
```

If validation succeeds, run the relevant models and tests:

``` bash
dbt run
dbt test
```

### 3. Start Airflow

If the repository's Docker Compose file defines the Airflow services,
run this from the directory containing that file:

``` bash
docker compose up -d
```

Open `http://localhost:8080` if that is the configured Airflow port.
Confirm that the DAG imports correctly and review task logs before
triggering a run.

### 4. Verify the Databricks job and output

-   Confirm the job ID in the DAG exists in the intended workspace.
-   Confirm the configured identity has permission to run it.
-   Trigger the DAG in Airflow.
-   Inspect the Airflow task logs and Databricks job run.
-   Verify expected schemas, tables, row counts, and data-quality
    results.

## Data Quality and Reliability

Recommended checks: - Compare source and target row counts. - Add
appropriate dbt tests for nulls, uniqueness, relationships, and accepted
values. - Check duplicate business keys and invalid timestamps. - Keep
Bronze records traceable to their source. - Document Silver cleaning and
standardization rules. - Reconcile Gold metrics against source-level
aggregates. - Use retries and actionable logs for transient failures. -
Make ingestion and transformations idempotent so reruns do not create
unintended duplicates. - If CDC is implemented, test inserts, updates,
deletes, and replay behavior.

Describe a check as implemented only after it exists in the code and has
been tested.

## Security

-   Never commit Databricks tokens, Neon passwords, AWS credentials, or
    secret connection strings.
-   Keep S3 Block Public Access enabled unless a separate, reviewed
    requirement justifies a change.
-   Follow least-privilege IAM permissions.
-   Restrict Unity Catalog grants to the required users and roles.
-   Rotate credentials exposed in logs, screenshots, chat, or Git
    history.
-   Use GitHub secret scanning and push protection as additional
    safeguards.

## Possible Extensions

-   Add or strengthen CDC and incremental ingestion.
-   Add freshness and source-to-target reconciliation checks.
-   Expand dbt tests and model documentation.
-   Add Airflow failure notifications and run-duration monitoring.
-   Add CI checks for Python, dbt parsing, and secret scanning.
-   Publish lineage and pipeline metrics.
-   Document a reproducible setup from a clean development environment.

## Summary

This project combines Neon PostgreSQL as a relational source, Amazon S3
as object storage, Databricks and Unity Catalog for lakehouse access and
processing, dbt for SQL transformation and data quality, and Apache
Airflow for orchestration. Bronze, Silver, and Gold layers organize data
from source-aligned records through cleaned datasets to analytics-ready
models. The supplied diagram additionally highlights CDC/file ingestion,
incremental processing, a One-Big-Table stage, and quality checks; their
implementation should be confirmed against the code.
