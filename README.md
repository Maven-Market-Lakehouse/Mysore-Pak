# Maven Market Lakehouse

Production-ready, single-environment Azure Databricks Lakehouse reference project for Maven Market.

## What this repository includes

- Unity Catalog-first governance model
- Medallion architecture (Bronze, Silver, Gold) with Delta Live Tables (DLT)
- Config-driven ingestion modules for CSV (ADLS), MongoDB, and Kafka
- Single source of runtime configuration: `config/config.yml`
- Custom structured logging framework (audit log sink in Unity Catalog)
- Databricks Asset Bundle (`databricks.yml`) with one target (`main`)
- Cluster policy definition for cost and compute governance
- GitHub Actions CI/CD workflow (lint, tests, bundle validate/deploy)
- Key Vault-backed secret scope integration points

## Repository structure

```text
maven-market-lakehouse/
  .github/workflows/deploy.yml
  config/config.yml
  databricks.yml
  cluster_policy.json
  sql/unity_catalog_setup.sql
  src/common/{config_loader.py,logger.py}
  src/ingestion/{csv_ingest.py,mongodb_ingest.py,kafka_ingest.py}
  src/dlt/{medallion_pipeline.py,bronze_template.py,silver_template.py,gold_template.py}
  tests/test_config_loader.py
```

## Quick start

1. Update `config/config.yml` with your workspace and source details.
2. Create and configure a Key Vault-backed Databricks secret scope as referenced in config.
3. Apply Unity Catalog bootstrap SQL from `sql/unity_catalog_setup.sql`.
4. Create cluster policy from `cluster_policy.json`.
5. Validate and deploy bundle:

```bash
databricks bundle validate -t main
databricks bundle deploy -t main
```

## Required secrets (example)

Store these in your Key Vault-backed Databricks secret scope:

- `mongo-username`
- `mongo-password`
- `kafka-username`
- `kafka-password`

## Notes

- This project intentionally uses one environment (`main`) while remaining modular and scalable.
- All table names, paths, and connection settings are managed in `config/config.yml`.
