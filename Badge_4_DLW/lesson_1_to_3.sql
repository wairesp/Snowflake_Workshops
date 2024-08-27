create or replace table util_db.public.my_data_types
(
  my_number number
, my_text varchar(10)
, my_bool boolean
, my_float float
, my_date date
, my_timestamp timestamp_tz
, my_variant variant
, my_array array
, my_object object
, my_geography geography
, my_geometry geometry
, my_vector vector(int,16)
);


CREATE DATABASE ZENAS_ATHLEISURE_DB;
DROP SCHEMA PUBLIC;
CREATE SCHEMA PRODUCTS;
CREATE STAGE PRODUCT_METADATA;


list @zenas_athleisure_db.products.product_metadata;
select $1
from @product_metadata/product_coordination_suggestions.txt; 
create file format zmd_file_format_1
RECORD_DELIMITER = '^';
select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);
create or replace file format zmd_file_format_2
FIELD_DELIMITER = '^';  
select $1, $2,$3,$4,$5,$6,$7,$8,$9,$10,$11
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

-- \** Make the Product Coordination Data Look great! **\

select $1
from @product_metadata/product_coordination_suggestions.txt; 

create or replace file format zmd_file_format_3
FIELD_DELIMITER = '=',
RECORD_DELIMITER = '^',
TRIM_SPACE = TRUE;

CREATE OR REPLACE VIEW zenas_athleisure_db.products.SWEATBAND_COORDINATION AS select REPLACE($1, '\r\n', '') AS "PRODUCT_CODE", $2 AS "HAS_MATCHING_SWEATSUIT"
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

-- \**  Make the sweatsuit_sizes look great  **\

select $1
from @product_metadata/sweatsuit_sizes.txt; 

create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';',
TRIM_SPACE = TRUE;

create or replace view ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES 
as select REPLACE($1, '\r\n', '') as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';


-- \**  Make the Sweatband Product Line File Look Great!  **\
select $1
from @product_metadata/swt_product_line.txt;

create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|',
RECORD_DELIMITER = ';',
TRIM_SPACE = TRUE; -- removes leading and trailing spaces

CREATE OR REPLACE VIEW  zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE 
AS select REPLACE($1, '\r\n', '') AS "PRODUCT_CODE", $2 AS "HEADBAND_DESCRIPTION", $3 AS "WRISTBAND_DESCRIPTION"
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2 );


-- wrap up
select product_code, has_matching_sweatsuit
from zenas_athleisure_db.products.sweatband_coordination;
select product_code, headband_description, wristband_description
from zenas_athleisure_db.products.sweatband_product_line;
select sizes_available
from zenas_athleisure_db.products.sweatsuit_sizes;

