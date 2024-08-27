-- into auto data unlimited account
CREATE DATABASE VIN;

USE VIN;

CREATE SCHEMA DECODE;

DROP SCHEMA PUBLIC;

--We need a table that will allow WMIs to be decoded into Manufacturer Name, Country and Vehicle Type
CREATE TABLE vin.decode.wmi_to_manuf 
(
     wmi	    varchar(6)
    ,manuf_id	    number(6)
    ,manuf_name	    varchar(50)
    ,country	    varchar(50)
    ,vehicle_type    varchar(50)
 );
 
--We need a table that will allow you to go from Manufacturer to Make
--For example, Mercedes AG of Germany and Mercedes USA both roll up into Mercedes
--But they use different WMI Codes
CREATE TABLE vin.decode.manuf_to_make
(
     manuf_id	number(6)
    ,make_name	varchar(50)
    ,make_id	number(5)
);

--We need a table that can decode the model year
-- The year 2001 is represented by the digit 1
-- The year 2020 is represented by the letter L
CREATE TABLE vin.decode.model_year
(
     model_year_code	varchar(1)
    ,model_year_name	varchar(4)
);

--We need a table that can decode which plant at which 
--the vehicle was assembled
--You might have code "A" for Honda and code "A" for Ford
--so you need both the Make and the Plant Code to properly decode 
--the plant code
CREATE TABLE vin.decode.manuf_plants
(
     make_id	number(5)
    ,plant_code	varchar(1)
    ,plant_name	varchar(75)
 );
 
--We need to use a combination of both the Make and VDS 
--to decode many attributes including the engine, transmission, etc
CREATE TABLE vin.decode.make_model_vds
(
     make_id	  number(3)
    ,model_id	  number(6)
    ,model_name	  varchar(50)
    ,vds	  varchar(5)
    ,desc1	  varchar(25)
    ,desc2	  varchar(25)
    ,desc3	  varchar(50)
    ,desc4	  varchar(25)
    ,desc5	  varchar(25)
    ,body_style	  varchar(25)
    ,engine	  varchar(100)
    ,drive_type	  varchar(50)
    ,transmission varchar(50)
    ,mpg  	varchar(25)
);


--Create a file format and then load each of the 5 Lookup Tables
--You need a file format if you want to load the table
CREATE FILE FORMAT vin.decode.comma_sep_oneheadrow 
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1 
field_optionally_enclosed_by = '"'  
trim_space = TRUE;


COPY INTO vin.decode.wmi_to_manuf
from @vin.decode.aws_s3_bucket
files = ('Maxs_WMIToManuf_data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

COPY INTO vin.decode.manuf_to_make
from @vin.decode.aws_s3_bucket
files = ('Maxs_ManufToMake_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);


COPY INTO vin.decode.model_year
from @vin.decode.aws_s3_bucket
files = ('Maxs_ModelYear_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

--there's a typo in the stage name here. Remember that AWS is case-sensitive and fix the file name
COPY INTO vin.decode.manuf_plants
from @vin.decode.aws_s3_bucket
files = ('Maxs_ManufPlants_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);

--there's one table left to load, and one file left to be loaded. 
--figure out what goes in each of the <bracketed> areas to make the final load
COPY INTO vin.decode.make_model_vds
from @vin.decode.aws_s3_bucket
files = ('Maxs_MMVDS_Data.csv')
file_format =(format_name = vin.decode.comma_sep_oneheadrow);


list @vin.decode.aws_s3_bucket;


--create a variable and set the value
set sample_vin = 'SAJAJ4FX8LCP55916';

--check to make sure you set the variable above
select $sample_vin;

--parse the vin into it's important pieces
SELECT $sample_vin as VIN
  , LEFT($sample_vin,3) as WMI
  , SUBSTR($sample_vin,4,5) as VDS
  , SUBSTR($sample_vin,10,1) as model_year_code
  , SUBSTR($sample_vin,11,1) as plant_code
;

-- This code must be run in the same worksheet (session) as the [set sample_vin =] statement, otherwise the variable will not 'exist'
select VIN
, manuf_name
, vehicle_type
, make_name
, plant_name
, model_year_name as model_year
, model_name
, desc1
, desc2
, desc3
, desc4
, desc5
, engine
, drive_type
, transmission
, mpg
from
  ( SELECT $sample_vin as VIN
  , LEFT($sample_vin,3) as WMI
  , SUBSTR($sample_vin,4,5) as VDS
  , SUBSTR($sample_vin,10,1) as model_year_code
  , SUBSTR($sample_vin,11,1) as plant_code
  ) vin
JOIN vin.decode.wmi_to_manuf w 
    ON vin.wmi = w.wmi
JOIN vin.decode.manuf_to_make m
    ON w.manuf_id=m.manuf_id
JOIN vin.decode.manuf_plants p
    ON vin.plant_code=p.plant_code
    AND m.make_id=p.make_id
JOIN vin.decode.model_year y
    ON vin.model_year_code=y.model_year_code
JOIN vin.decode.make_model_vds vds
    ON vds.vds=vin.vds 
    AND vds.make_id = m.make_id;

--This will get the outline of the function ready to go
--notice that we added "or replace" and "secure" to this code that was not shown in the screenshot
create or replace secure function vin.decode.parse_and_enhance_vin(this_vin varchar(25))
returns table (
    VIN varchar(25)
    , manuf_name varchar(25)
    , vehicle_type varchar(25)
    , make_name varchar(25)
    , plant_name varchar(25)
    , model_year varchar(25)
    , model_name varchar(25)
    , desc1 varchar(25)
    , desc2 varchar(25)
    , desc3 varchar(25)
    , desc4 varchar(25)
    , desc5 varchar(25)
    , engine varchar(25)
    , drive_type varchar(25)
    , transmission varchar(25)
    , mpg varchar(25)
)
as $$
select VIN
, manuf_name
, vehicle_type
, make_name
, plant_name
, model_year_name as model_year
, model_name
, desc1
, desc2
, desc3
, desc4
, desc5
, engine
, drive_type
, transmission
, mpg
from
( SELECT this_vin as VIN
, LEFT(this_vin,3) as WMI
, SUBSTR(this_vin,4,5) as VDS
, SUBSTR(this_vin,10,1) as model_year_code
, SUBSTR(this_vin,11,1) as plant_code
) vin
JOIN vin.decode.wmi_to_manuf w 
    ON vin.wmi = w.wmi
JOIN vin.decode.manuf_to_make m
    ON w.manuf_id=m.manuf_id
JOIN vin.decode.manuf_plants p
    ON vin.plant_code=p.plant_code
    AND m.make_id=p.make_id
JOIN vin.decode.model_year y
    ON vin.model_year_code=y.model_year_code
JOIN vin.decode.make_model_vds vds
    ON vds.vds=vin.vds 
    AND vds.make_id = m.make_id
$$;


-- Use the select statement from the previous page and paste it into the function.
-- Then you will need to make a few small changes to get it to work. 
--In each function call below, we pass in a different VIN as THIS_VIN
select *
from table(vin.decode.PARSE_AND_ENHANCE_VIN('SAJAJ4FX8LCP55916'));

select *
from table(vin.decode.PARSE_AND_ENHANCE_VIN('19UUB2F34LA001631'));
 
select *
from table(vin.decode.PARSE_AND_ENHANCE_VIN('5UXCR6C0XL9C77256'));
