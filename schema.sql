-- Source DDL for the MarketArm Databricks App.

CREATE TABLE spark_catalog.marketarm.tblusers (
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

CREATE TABLE spark_catalog.marketarm.tblentry (
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
    userID STRING,
    Description STRING,
    Active BOOLEAN)
USING delta
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4');
