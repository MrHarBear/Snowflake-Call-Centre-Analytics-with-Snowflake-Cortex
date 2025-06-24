/***************************************************************************************************
| CALL CENTER ANALYTICS WITH SNOWFLAKE CORTEX AI - SETUP
Demo:         Call Center Audio Transcription and AI Analytics
Create Date:  2025-01-16
Purpose:      Complete environment setup for call center analytics demo
Data Source:  GitHub Repository Integration with Audio Files
Customer:     Snowflake AI Capabilities Demo - Competing with GONG
****************************************************************************************************

/*----------------------------------------------------------------------------------
This script provides comprehensive environment setup for the call center analytics
demo showcasing Snowflake's audio transcription and AI capabilities. It establishes
the foundational infrastructure including databases, warehouses, stages, Git 
integration, and audio file ingestion.

Key Concepts:
  • Environment Setup: Database, schema, and warehouse configuration
  • Git Integration: Repository connection for automated file management
  • Audio File Staging: Internal stages for English MP3 files only
  • AI Function Access: Cortex function privileges and cross-region setup
  • Resource Management: Simplified warehouse structure for demo efficiency
----------------------------------------------------------------------------------*/

-- SETUP
USE ROLE ACCOUNTADMIN;

-- Cross-region access (commented out for now, uncomment if needed)
-- ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Create database and schema
CREATE DATABASE IF NOT EXISTS CALL_CENTER_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS AUDIO_PROCESSING;
USE DATABASE CALL_CENTER_ANALYTICS;
USE SCHEMA AUDIO_PROCESSING;

-- Create warehouses (simplified to 2 warehouses)
CREATE OR REPLACE WAREHOUSE AUDIO_CORTEX_WH
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60
RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'
COMMENT = 'Warehouse for audio transcription and AI processing';

CREATE OR REPLACE WAREHOUSE AUDIO_STREAMLIT_WH
WAREHOUSE_SIZE = XSMALL
AUTO_SUSPEND = 60
RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'
COMMENT = 'Warehouse for Streamlit visualization';

USE WAREHOUSE AUDIO_CORTEX_WH;

-- Create stage for audio files
CREATE STAGE CALL_CENTER_AUDIO_FILES 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
	COMMENT = 'Stage for call center audio files';

-- -- Git Integration Setup
-- CREATE OR REPLACE API INTEGRATION git_api_integration
--   API_PROVIDER = git_https_api
--   API_ALLOWED_PREFIXES = ('https://github.com')
--   ENABLED = TRUE
--   COMMENT = 'Git API integration for repository access';

desc INTEGRATION GITHUB_INTEGRATION_CORTEX_AGENTS_DEMO;

-- Create Git repository integration
CREATE OR REPLACE GIT REPOSITORY CALL_CENTER_REPO
    API_INTEGRATION = GITHUB_INTEGRATION_CORTEX_AGENTS_DEMO
    ORIGIN = 'https://github.com/MrHarBear/Snowflake-Call-Centre-Analytics-with-Snowflake-Cortex.git'
    GIT_CREDENTIALS = NULL
    COMMENT = 'Git repository for call center analytics demo';

-- Refresh the Git repository clone
ALTER GIT REPOSITORY CALL_CENTER_REPO FETCH;

-- List files to verify the repository connection
SHOW GIT BRANCHES IN GIT REPOSITORY CALL_CENTER_REPO;
LS @CALL_CENTER_REPO/branches/main;

-- Copy English MP3 audio files only (2 files expected)
COPY FILES
  INTO @CALL_CENTER_AUDIO_FILES
  FROM '@CALL_CENTER_REPO/branches/main/audio_files/'
  PATTERN='.*\.mp3'
  FILES = ('Health-Insurance-1.mp3', 'Sample_ATT_Inbound_Call-MONO_47sec.mp3');

CREATE NOTEBOOK CORTEX_AI_ANALYTICS
 FROM '@CALL_CENTER_REPO/branches/main/'
 MAIN_FILE = '02_CORTEX_AI_ANALYTICS.ipynb'
 QUERY_WAREHOUSE = AUDIO_CORTEX_WH;

-- Verify files are loaded
SELECT * FROM DIRECTORY('@CALL_CENTER_AUDIO_FILES');

-- Create Customer Email table for comprehensive AI analytics
CREATE OR REPLACE TABLE CUSTOMER_EMAILS (
    EMAIL_ID VARCHAR(50),
    DATE_RECEIVED TIMESTAMP,
    CUSTOMER_ID VARCHAR(50),
    EMAIL_CONTENTS TEXT,
    INGESTION_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create stage for customer emails CSV file
CREATE OR REPLACE STAGE CUSTOMER_EMAIL_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Stage for customer care email CSV files';

-- Copy customer email CSV file from repository
COPY FILES
    INTO @CUSTOMER_EMAIL_STAGE
    FROM '@CALL_CENTER_REPO/branches/main/Customer Care Emails/'
    --PATTERN='.*\.csv'
    FILES = ('Email _ Contact Center Analysis.csv');

-- Create file format for CSV ingestion
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('NULL', 'null', '');

-- Load customer email data from CSV
COPY INTO CUSTOMER_EMAILS(EMAIL_ID, DATE_RECEIVED, CUSTOMER_ID, EMAIL_CONTENTS)
FROM (
    SELECT 
        $1 AS EMAIL_ID,
        TRY_TO_TIMESTAMP($2) AS DATE_RECEIVED,
        $3 AS CUSTOMER_ID,
        $4 AS EMAIL_CONTENTS
    FROM '@CUSTOMER_EMAIL_STAGE/Email _ Contact Center Analysis.csv'
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

/*----------------------------------------------------------------------------------
CUSTOMER DEMOGRAPHICS AND PURCHASE HISTORY SETUP
Ingests customer profile data and vehicle purchase history for comprehensive
customer 360 analytics and AI-powered insights
----------------------------------------------------------------------------------*/

-- Create Customer Demographics table
CREATE OR REPLACE TABLE CUSTOMER_DEMOGRAPHICS (
    CUSTOMER_ID VARCHAR(50),
    CUSTOMER_NAME VARCHAR(100),
    EMAIL VARCHAR(100),
    AGE NUMBER(3),
    GENDER VARCHAR(10),
    MARITAL_STATUS VARCHAR(20),
    HOUSEHOLD_INCOME_GBP NUMBER(10),
    OCCUPATION VARCHAR(100),
    EDUCATION_LEVEL VARCHAR(50),
    LOCATION_CITY VARCHAR(100),
    LOCATION_REGION VARCHAR(50),
    POSTCODE VARCHAR(20),
    HOME_OWNERSHIP VARCHAR(50),
    CREDIT_SCORE NUMBER(4),
    HOUSEHOLD_SIZE NUMBER(2),
    CHILDREN_COUNT NUMBER(2),
    DRIVING_EXPERIENCE_YEARS NUMBER(3),
    PRIMARY_USE_CASE VARCHAR(50),
    PREFERRED_FUEL_TYPE VARCHAR(20),
    BUDGET_RANGE_GBP VARCHAR(50),
    COMMUNICATION_PREFERENCE VARCHAR(20),
    MARKETING_CONSENT VARCHAR(10),
    LOYALTY_PROGRAM_MEMBER VARCHAR(10),
    LAST_CONTACT_DATE DATE,
    CUSTOMER_LIFETIME_VALUE_GBP NUMBER(10),
    INGESTION_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create Vehicle Purchase History table
CREATE OR REPLACE TABLE VEHICLE_PURCHASE_HISTORY (
    TRANSACTION_ID VARCHAR(50),
    CUSTOMER_ID VARCHAR(50),
    VEHICLE_MODEL VARCHAR(50),
    VEHICLE_YEAR NUMBER(4),
    PURCHASE_DATE DATE,
    SALE_PRICE_GBP NUMBER(10),
    DEALERSHIP_LOCATION VARCHAR(50),
    SALES_AGENT VARCHAR(100),
    PURCHASE_TYPE VARCHAR(20),
    DELIVERY_DATE DATE,
    MILEAGE_AT_PURCHASE NUMBER(10),
    VEHICLE_STATUS VARCHAR(20),
    CURRENT_MILEAGE NUMBER(10),
    LAST_SERVICE_DATE DATE,
    INGESTION_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create stage for customer data CSV files
CREATE OR REPLACE STAGE CUSTOMER_DATA_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Stage for customer demographics and purchase history CSV files';

-- Copy customer data CSV files from repository
COPY FILES
    INTO @CUSTOMER_DATA_STAGE
    FROM '@CALL_CENTER_REPO/branches/main/'
    FILES = ('customer_demographics.csv', 'vehicle_purchase_history.csv');

-- Load customer demographics data from CSV
COPY INTO CUSTOMER_DEMOGRAPHICS(
    CUSTOMER_ID, CUSTOMER_NAME, EMAIL, AGE, GENDER, MARITAL_STATUS, 
    HOUSEHOLD_INCOME_GBP, OCCUPATION, EDUCATION_LEVEL, LOCATION_CITY, LOCATION_REGION,
    POSTCODE, HOME_OWNERSHIP, CREDIT_SCORE, HOUSEHOLD_SIZE, CHILDREN_COUNT,
    DRIVING_EXPERIENCE_YEARS, PRIMARY_USE_CASE, PREFERRED_FUEL_TYPE, 
    BUDGET_RANGE_GBP, COMMUNICATION_PREFERENCE, MARKETING_CONSENT, 
    LOYALTY_PROGRAM_MEMBER, LAST_CONTACT_DATE, CUSTOMER_LIFETIME_VALUE_GBP
)
FROM (
    SELECT 
        $1 AS CUSTOMER_ID,
        $2 AS CUSTOMER_NAME,
        $3 AS EMAIL,
        TRY_TO_NUMBER($4) AS AGE,
        $5 AS GENDER,
        $6 AS MARITAL_STATUS,
        TRY_TO_NUMBER($7) AS HOUSEHOLD_INCOME_GBP,
        $8 AS OCCUPATION,
        $9 AS EDUCATION_LEVEL,
        $10 AS LOCATION_CITY,
        $11 AS LOCATION_REGION,
        $12 AS POSTCODE,
        $13 AS HOME_OWNERSHIP,
        TRY_TO_NUMBER($14) AS CREDIT_SCORE,
        TRY_TO_NUMBER($15) AS HOUSEHOLD_SIZE,
        TRY_TO_NUMBER($16) AS CHILDREN_COUNT,
        TRY_TO_NUMBER($17) AS DRIVING_EXPERIENCE_YEARS,
        $18 AS PRIMARY_USE_CASE,
        $19 AS PREFERRED_FUEL_TYPE,
        $20 AS BUDGET_RANGE_GBP,
        $21 AS COMMUNICATION_PREFERENCE,
        $22 AS MARKETING_CONSENT,
        $23 AS LOYALTY_PROGRAM_MEMBER,
        TRY_TO_DATE($24) AS LAST_CONTACT_DATE,
        TRY_TO_NUMBER($25) AS CUSTOMER_LIFETIME_VALUE_GBP
    FROM '@CUSTOMER_DATA_STAGE/customer_demographics.csv'
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load vehicle purchase history data from CSV
COPY INTO VEHICLE_PURCHASE_HISTORY(
    TRANSACTION_ID, CUSTOMER_ID, VEHICLE_MODEL, VEHICLE_YEAR, PURCHASE_DATE,
    SALE_PRICE_GBP, DEALERSHIP_LOCATION, SALES_AGENT, PURCHASE_TYPE, 
    DELIVERY_DATE, MILEAGE_AT_PURCHASE, VEHICLE_STATUS, CURRENT_MILEAGE, LAST_SERVICE_DATE
)
FROM (
    SELECT 
        $1 AS TRANSACTION_ID,
        $2 AS CUSTOMER_ID,
        NULLIF($3, 'None') AS VEHICLE_MODEL,
        CASE WHEN TRY_TO_NUMBER($4) = 0 THEN NULL ELSE TRY_TO_NUMBER($4) END AS VEHICLE_YEAR,
        CASE WHEN TRY_TO_DATE($5) = '1900-01-01' THEN NULL ELSE TRY_TO_DATE($5) END AS PURCHASE_DATE,
        CASE WHEN TRY_TO_NUMBER($6) = 0 THEN NULL ELSE TRY_TO_NUMBER($6) END AS SALE_PRICE_GBP,
        $7 AS DEALERSHIP_LOCATION,
        $8 AS SALES_AGENT,
        $9 AS PURCHASE_TYPE,
        CASE WHEN TRY_TO_DATE($10) = '1900-01-01' THEN NULL ELSE TRY_TO_DATE($10) END AS DELIVERY_DATE,
        TRY_TO_NUMBER($11) AS MILEAGE_AT_PURCHASE,
        $12 AS VEHICLE_STATUS,
        TRY_TO_NUMBER($13) AS CURRENT_MILEAGE,
        CASE WHEN $14 = 'Pending' OR $14 = '0' THEN NULL ELSE TRY_TO_DATE($14) END AS LAST_SERVICE_DATE
    FROM '@CUSTOMER_DATA_STAGE/vehicle_purchase_history.csv'
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- List available audio files for confirmation
SELECT 
    RELATIVE_PATH AS FILENAME,
    SIZE AS FILE_SIZE_BYTES,
    LAST_MODIFIED
FROM DIRECTORY('@CALL_CENTER_AUDIO_FILES')
WHERE RELATIVE_PATH LIKE '%.mp3'
ORDER BY RELATIVE_PATH;

-- Display sample email data
SELECT 
    EMAIL_ID,
    DATE_RECEIVED,
    CUSTOMER_ID,
    LEFT(EMAIL_CONTENTS, 100) || '...' AS EMAIL_PREVIEW
FROM CUSTOMER_EMAILS
ORDER BY DATE_RECEIVED DESC
LIMIT 5;

-- Display sample customer demographics data
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    AGE,
    LOCATION_CITY,
    LOCATION_REGION,
    OCCUPATION,
    HOUSEHOLD_INCOME_GBP,
    LOYALTY_PROGRAM_MEMBER
FROM CUSTOMER_DEMOGRAPHICS
ORDER BY CUSTOMER_ID
LIMIT 10;

-- Display sample purchase history data
SELECT 
    TRANSACTION_ID,
    CUSTOMER_ID,
    VEHICLE_MODEL,
    VEHICLE_YEAR,
    PURCHASE_DATE,
    SALE_PRICE_GBP,
    DEALERSHIP_LOCATION,
    VEHICLE_STATUS
FROM VEHICLE_PURCHASE_HISTORY
WHERE VEHICLE_MODEL IS NOT NULL
ORDER BY PURCHASE_DATE DESC
LIMIT 10;

-- Verify data counts
SELECT 'CUSTOMER_EMAILS' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM CUSTOMER_EMAILS
UNION ALL
SELECT 'CUSTOMER_DEMOGRAPHICS' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM CUSTOMER_DEMOGRAPHICS
UNION ALL  
SELECT 'VEHICLE_PURCHASE_HISTORY' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM VEHICLE_PURCHASE_HISTORY
ORDER BY TABLE_NAME; 