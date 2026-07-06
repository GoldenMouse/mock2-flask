-- Databricks notebook source
-- MAGIC %md
-- MAGIC # MarketArm — Table Initialization
-- MAGIC
-- MAGIC Creates the `spark_catalog.marketarm` schema and the two Delta tables the
-- MAGIC Entity Manager app uses (`tblusers`, `tblentry`), then optionally seeds
-- MAGIC sample rows.
-- MAGIC
-- MAGIC **How to run:** import this file into your Databricks workspace
-- MAGIC (Workspace → Import → File), attach it to a cluster or SQL warehouse, and
-- MAGIC use **Run All** — or run the cells one at a time.
-- MAGIC
-- MAGIC The `CREATE TABLE` statements use `IF NOT EXISTS`, so this notebook is safe
-- MAGIC to re-run. The seed cells near the bottom are optional — skip them if you
-- MAGIC only want empty tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS spark_catalog.marketarm;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS spark_catalog.marketarm.tblusers (
    userID BIGINT,
    userFirst STRING,
    userLast STRING,
    email STRING,
    userName STRING,
    userPassword STRING,
    accessLevel BIGINT,
    active BOOLEAN)
USING delta
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS spark_catalog.marketarm.tblentry (
    entryID BIGINT,
    category STRING,
    nameEntry STRING,
    citizen STRING,
    alias STRING,
    alias2 STRING,
    alias3 STRING,
    URL STRING,
    PriorWCDJ STRING,
    LastWCDJ TIMESTAMP,
    userID STRING,           -- creator identity (who created the entry)
    Description STRING,
    Active BOOLEAN,
    createdAt TIMESTAMP,     -- when the entry was created
    updatedBy STRING,        -- who last modified the entry
    updatedAt TIMESTAMP)     -- when the entry was last modified
USING delta
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. (Optional) Seed sample entries
-- MAGIC
-- MAGIC Mirrors the mock rows the app ships with for local development, so a freshly
-- MAGIC deployed app has data to show. Skip this cell for empty tables.
-- MAGIC
-- MAGIC `tblentry` has no identity column (the app allocates `entryID` as
-- MAGIC `MAX(entryID) + 1`), so the ids are set explicitly here.

-- COMMAND ----------

INSERT INTO spark_catalog.marketarm.tblentry
    (entryID, category, nameEntry, citizen, alias, alias2, alias3, URL, PriorWCDJ, LastWCDJ, userID, Description, Active, createdAt, updatedBy, updatedAt)
VALUES
    (1, 'Individual', 'Ivan Petrov', 'Russia', 'I. Petrov', 'Vanya', NULL,
     'https://example.gov/sanctions/1001', '2023-11-04', TIMESTAMP '2024-06-12 09:30:00',
     'analyst01', 'Listed for procurement network activity.', true,
     TIMESTAMP '2024-06-12 09:30:00', 'analyst01', TIMESTAMP '2024-06-12 09:30:00'),
    (2, 'Entity', 'Northwind Trading LLC', 'UAE', 'Northwind Ltd', NULL, NULL,
     'https://example.gov/sanctions/1002', NULL, TIMESTAMP '2024-02-01 14:15:00',
     'analyst02', 'Front company; shipping intermediary.', true,
     TIMESTAMP '2024-02-01 14:15:00', 'analyst02', TIMESTAMP '2024-02-01 14:15:00'),
    (3, 'Vessel', 'MV Sea Falcon', 'Panama', 'Sea Falcon', 'IMO 9345678', NULL,
     NULL, '2022-08-19', TIMESTAMP '2023-12-20 00:00:00',
     'analyst01', 'Delisted after review.', false,
     TIMESTAMP '2022-08-19 08:00:00', 'analyst03', TIMESTAMP '2023-12-20 16:45:00');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. (Optional) Seed a demo user
-- MAGIC
-- MAGIC The app authenticates via Databricks Apps workspace SSO, **not** via
-- MAGIC `tblusers`, so this row is illustrative only. If you later drive access
-- MAGIC levels from `tblusers`, store a properly hashed value in `userPassword` —
-- MAGIC never a plaintext password.

-- COMMAND ----------

INSERT INTO spark_catalog.marketarm.tblusers
    (userID, userFirst, userLast, email, userName, userPassword, accessLevel, active)
VALUES
    (1, 'Demo', 'Analyst', 'demo.analyst@example.com', 'danalyst', '<hashed-password>', 9, true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Verify

-- COMMAND ----------

SELECT 'tblentry' AS table_name, COUNT(*) AS row_count FROM spark_catalog.marketarm.tblentry
UNION ALL
SELECT 'tblusers' AS table_name, COUNT(*) AS row_count FROM spark_catalog.marketarm.tblusers;

-- COMMAND ----------

SELECT * FROM spark_catalog.marketarm.tblentry ORDER BY entryID;
