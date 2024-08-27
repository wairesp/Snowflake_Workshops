/* Create another database and warehouse */
use role SYSADMIN;

create database INTL_DB;

use schema INTL_DB.PUBLIC;



/*Create a warehouse for loading INTL_DB  */

use role SYSADMIN;

create warehouse INTL_WH 
with 
warehouse_size = 'XSMALL' 
warehouse_type = 'STANDARD' 
auto_suspend = 600 --600 seconds/10 mins
auto_resume = TRUE;

use warehouse INTL_WH;


/*Create Table INT_STDS_ORG_3166 */


create or replace table intl_db.public.INT_STDS_ORG_3166 
(iso_country_name varchar(100), 
 country_name_official varchar(200), 
 sovreignty varchar(40), 
 alpha_code_2digit varchar(2), 
 alpha_code_3digit varchar(3), 
 numeric_country_code integer,
 iso_subdivision varchar(15), 
 internet_domain_code varchar(10)
);

create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR 
  type = 'CSV' --use CSV for any flat file
  compression = 'AUTO' 
  field_delimiter = '|' --pipe or vertical bar
  record_delimiter = '\r' --carriage return
  skip_header = 1  --1 header row
  field_optionally_enclosed_by = '\042'  --double quotes
  trim_space = FALSE;

/*Check That You Created and Loaded the Table Properly */

select count(*) as found, '249' as expected 
from INTL_DB.PUBLIC.INT_STDS_ORG_3166; 

select row_count
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';



/* JOIN SHARED DATA WITH LOCAL DATA */

select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;


create view intl_db.public.NATIONS_SAMPLE_PLUS_ISO 
( iso_country_name
  ,country_name_official
  ,alpha_code_2digit
  ,region) AS
  select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
    from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
    left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
    on upper(i.iso_country_name)= n.n_name
    left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
    on n_regionkey = r_regionkey;
;

select *
from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;

-- Marting Figures Out How To Add Views

-- Convert "Regular" Views to Secure Views
alter view intl_db.public.NATIONS_SAMPLE_PLUS_ISO
set secure; 

alter view intl_db.public.SIMPLE_CURRENCY
set secure; 

-- Create a few more tables and load them
create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
  comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

--  Create Table Country to Currency
  create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
  (
    country_char_code varchar(3), 
    country_numeric_code integer, 
    country_name varchar(100), 
    currency_name varchar(100), 
    currency_char_code varchar(3), 
    currency_numeric_code integer
  ) 
  comment = 'Mapping table currencies to countries';


/*Create a File Format to Process files with Commas, Linefeeds and a Header Row */
 create file format util_db.public.CSV_COMMA_LF_HEADER
  type = 'CSV' 
  field_delimiter = ',' 
  record_delimiter = '\n' -- the n represents a Line Feed character
  skip_header = 1 
;

-- Load the Currencies Table

copy into  intl_db.public.CURRENCIES 
from @util_db.public.aws_s3_bucket
files = ( 'currencies.csv')
file_format = ( format_name=util_db.public.CSV_COMMA_LF_HEADER );

-- Load the Country to Currency Table
list @util_db.public.aws_s3_bucket;
copy into  intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE
from @util_db.public.aws_s3_bucket
files = ( 'country_code_to_currency_code.csv')
file_format = ( format_name=util_db.public.CSV_COMMA_LF_HEADER );


-- View the Currencies Table
CREATE VIEW SIMPLE_CURRENCY AS
select COUNTRY_CHAR_CODE AS CTY_CODE,
CURRENCY_CHAR_CODE AS CUR_CODE,
from intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE;

-- Test view
select * from SIMPLE_CURRENCY;



-- LISTING PRIVILEGES 
GRANT IMPORT SHARE ON ACCOUNT TO ROLE SYSADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE SYSADMIN;