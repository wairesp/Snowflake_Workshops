/*  Create a Table Raw JSON Data */

// JSON DDL Scripts
use database library_card_catalog;
use role sysadmin;

// Create an Ingestion Table for JSON Data
create or replace table library_card_catalog.public.author_ingest_json
(
  raw_author variant
);

/* File Format Creation */

//Create File Format for JSON Data 
create or REPLACE file format library_card_catalog.public.json_file_format
type = 'JSON' 
compression = 'AUTO' 
enable_octal = FALSE
allow_duplicate = FALSE 
strip_outer_array = TRUE
strip_null_values = FALSE 
ignore_utf8_errors = FALSE; 


//QUERYING THE FORMAT OF THE JSON FILE

--The data in the file, with no FILE FORMAT specifiedselect $1, $2, $3SELECT
-- Use the appropriate role
USE ROLE accountadmin;

-- Query the JSON data using the file format
SELECT
    $1
FROM @util_db.public.my_internal_stage/author_with_header.json
(FILE_FORMAT => library_card_catalog.public.json_file_format);

/*** COPY INTO */
copy into library_card_catalog.public.author_ingest_json
from @util_db.public.my_internal_stage
files = ( 'author_with_header.json')
file_format = ( format_name = json_file_format);


/*** Quering the json table */
//returns AUTHOR_UID value from top-level object's attribute
select raw_author:AUTHOR_UID
from author_ingest_json;

//returns the data in a way that makes it look like a normalized table
SELECT 
raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;

