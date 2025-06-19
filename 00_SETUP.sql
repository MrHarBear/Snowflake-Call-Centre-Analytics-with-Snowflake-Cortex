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

-- Git Integration Setup
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com')
  ENABLED = TRUE
  COMMENT = 'Git API integration for repository access';

DESC INTEGRATION git_api_integration;

-- Create Git repository integration
CREATE OR REPLACE GIT REPOSITORY CALL_CENTER_REPO
    API_INTEGRATION = git_api_integration
    ORIGIN = 'https://github.com/hchen/Documents/GitHub/Snowflake-Call-Centre-Analytics-with-Snowflake-Cortex.git'
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

-- Verify files are loaded
SELECT * FROM DIRECTORY('@CALL_CENTER_AUDIO_FILES');

-- Grant necessary privileges for AI functions
GRANT USAGE ON DATABASE CALL_CENTER_ANALYTICS TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA CALL_CENTER_ANALYTICS.AUDIO_PROCESSING TO ROLE SYSADMIN;
GRANT USAGE, OPERATE ON WAREHOUSE AUDIO_CORTEX_WH TO ROLE SYSADMIN;
GRANT USAGE, OPERATE ON WAREHOUSE AUDIO_STREAMLIT_WH TO ROLE SYSADMIN;
GRANT READ ON STAGE CALL_CENTER_AUDIO_FILES TO ROLE SYSADMIN;

-- Set up SQL variable for current user
SET MY_USER_ID = CURRENT_USER();

-- Display setup completion summary
SELECT 
    'Setup completed successfully!' AS STATUS,
    CURRENT_DATABASE() AS DATABASE_NAME,
    CURRENT_SCHEMA() AS SCHEMA_NAME,
    CURRENT_WAREHOUSE() AS WAREHOUSE_NAME,
    CURRENT_TIMESTAMP() AS SETUP_TIMESTAMP;

-- List available audio files for confirmation
SELECT 
    'Available Audio Files:' AS SECTION,
    RELATIVE_PATH AS FILENAME,
    SIZE AS FILE_SIZE_BYTES,
    LAST_MODIFIED
FROM DIRECTORY('@CALL_CENTER_AUDIO_FILES')
WHERE RELATIVE_PATH LIKE '%.mp3'
ORDER BY RELATIVE_PATH; 