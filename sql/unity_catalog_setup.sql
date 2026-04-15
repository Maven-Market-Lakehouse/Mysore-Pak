-- Unity Catalog bootstrap
CREATE CATALOG IF NOT EXISTS maven_market_uc;

CREATE SCHEMA IF NOT EXISTS maven_market_uc.bronze;
CREATE SCHEMA IF NOT EXISTS maven_market_uc.silver;
CREATE SCHEMA IF NOT EXISTS maven_market_uc.gold;
CREATE SCHEMA IF NOT EXISTS maven_market_uc.audit;

-- Audit table for custom application logs
CREATE TABLE IF NOT EXISTS maven_market_uc.audit.audit_logs (
  event_time_utc TIMESTAMP,
  event_type STRING,
  stage STRING,
  error_type STRING,
  error_message STRING,
  traceback STRING,
  metadata STRING
) USING DELTA;

-- Access controls (adjust group names as needed)
GRANT USE CATALOG ON CATALOG maven_market_uc TO `grp_data_engineers`;
GRANT USE CATALOG ON CATALOG maven_market_uc TO `grp_analysts`;

GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA maven_market_uc.bronze TO `grp_data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA maven_market_uc.silver TO `grp_data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA maven_market_uc.gold TO `grp_data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA maven_market_uc.audit TO `grp_data_engineers`;

GRANT USE SCHEMA ON SCHEMA maven_market_uc.gold TO `grp_analysts`;
GRANT SELECT ON SCHEMA maven_market_uc.gold TO `grp_analysts`;

-- Row-level policy example for region-based filtering.
CREATE OR REPLACE FUNCTION maven_market_uc.gold.current_user_region()
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('grp_data_platform_admins') THEN NULL
  ELSE regexp_extract(current_user(), 'region_([A-Za-z0-9_]+)', 1)
END;

-- Dynamic masking example for sensitive columns.
CREATE OR REPLACE FUNCTION maven_market_uc.silver.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('grp_data_platform_admins') THEN email
  ELSE concat('***@', split(email, '@')[1])
END;
