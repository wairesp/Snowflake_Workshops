/* Granting The Same Rights - But Using Code */
grant imported privileges
on database SNOWFLAKE_SAMPLE_DATA
to role SYSADMIN;


--Check the range of values in the Market Segment Column
SELECT DISTINCT c_mktsegment
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segments have the most customers
SELECT c_mktsegment, COUNT(*)
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
GROUP BY c_mktsegment
ORDER BY COUNT(*);


-- Nations Table
SELECT N_NATIONKEY, N_NAME, N_REGIONKEY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

-- Regions Table
SELECT R_REGIONKEY, R_NAME
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- Join the Tables and Sort
SELECT R_NAME as Region, N_NAME as Nation
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
ORDER BY R_NAME, N_NAME ASC;

--Group and Count Rows Per Region
SELECT R_NAME as Region, count(N_NAME) as NUM_COUNTRIES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
GROUP BY R_NAME;

 /* Create a Local Database Named UTIL_DB */

-- where did you put the function?
show user functions in account;

-- did you put it here?
select * 
from util_db.information_schema.functions
where function_name = 'GRADER'
and function_catalog = 'UTIL_DB'
and function_owner = 'ACCOUNTADMIN';

grant usage 
on function UTIL_DB.PUBLIC.GRADER(VARCHAR, BOOLEAN, NUMBER, NUMBER, VARCHAR) 
to sysadmin;

show stages in account; 

create stage util_db.public.aws_s3_bucket url = 's3://uni-cmcw';

/*grant usage to sysadmin */
GRANT USAGE ON STAGE util_db.public.aws_s3_bucket TO ROLE sysadmin;

list @util_db.public.aws_s3_bucket;

copy into  intl_db.public.INT_STDS_ORG_3166 
from @util_db.public.aws_s3_bucket
files = ( 'ISO_Countries_UTF8_pipe.csv')
file_format = ( format_name=util_db.public.PIPE_DBLQUOTE_HEADER_CR );



