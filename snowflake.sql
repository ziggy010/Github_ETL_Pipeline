-- Create a database for the GitHub pipeline
CREATE DATABASE github_pipeline_db;

-- Create or replace an external stage for GitHub data
CREATE OR REPLACE STAGE github_stage
    STORAGE_INTEGRATION = github_etl_init
    URL = 's3://snowflake-db-tutorial-ziggy/github_data/'
    FILE_FORMAT = (FORMAT_NAME = MANAGE_DB.FILE_FORMATS.CSV_FILE_FORMAT);

-- Create or replace a table to store GitHub commit data
CREATE OR REPLACE TABLE github_data (
    name STRING,
    email STRING,
    commit_date DATE,
    commit_time TIME
);

-- Query all data from the github_data table
SELECT * FROM github_data;

-- Create or replace a pipe for automated ingestion from the stage
CREATE OR REPLACE PIPE github_data_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO github_data
    FROM @github_stage/transformed_data/github_transformed;

-- Analyze commit data by counting commits per author, ordered by frequency
SELECT name, COUNT(name) AS commit_count
FROM github_data
GROUP BY name
ORDER BY commit_count DESC;