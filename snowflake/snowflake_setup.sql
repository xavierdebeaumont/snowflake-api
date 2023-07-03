USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE DATA_API_WH WITH WAREHOUSE_SIZE='medium';

CREATE OR REPLACE ROLE DATA_API_ROLE;

GRANT USAGE ON WAREHOUSE DATA_API_WH TO ROLE DATA_API_ROLE;

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE DATA_API_ROLE;

CREATE OR REPLACE USER "DATA_API" PASSWORD = '<SNOWFLAKE USER PASSWORD>' DEFAULT_ROLE = DATA_API_ROLE DEFAULT_WAREHOUSE = DATA_API_WH DEFAULT_NAMESPACE = SNOWFLAKE_SAMPLE_DATA.TPCH_SF10 MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE DATA_API_ROLE TO USER DATA_API;