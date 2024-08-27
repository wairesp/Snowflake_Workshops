-- Denver's Confluence Park
-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lng='-104.97300245114094';
set mc_lat='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lng='-105.00840763333615'; 
set loc_lat='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lng,$mc_lat) as melanies_cafe_point;
select st_makepoint($loc_lng,$loc_lat) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lng,$mc_lat)
        ,st_makepoint($loc_lng,$loc_lat)
        ) as mc_to_cp;
     

-- 📓 Variables are Cool, But Constants Aren't So Bad!
/*
Variables are versatile, letting you reuse code with different inputs for varied results. 
Constants, like 360 degrees in a circle or π as 3.14, are fixed. When calculating the distance to Melanie's Cafe,
 you can use constants for its coordinates instead of variables. */
select st_distance(
    st_makepoint($mc_lng,$mc_lat),
    st_makepoint($loc_lng,$loc_lat)
    ) as mc_to_cp;


select st_distance(
    st_makepoint(-104.97300245114094,39.76471253574085),
    st_makepoint($loc_lng,$loc_lat)
    ) as mc_to_cp;


-- 📓 Let's Create a UDF for Measuring Distance from Melanie's Café
-- 🥋 Filling in the Function Code
USE ROLE SYSADMIN;
create schema mels_smoothie_challenge_db.LOCATIONS;

CREATE or REPLACE FUNCTION mels_smoothie_challenge_db.LOCATIONS.distance_to_mc(loc_lng number(38,32), loc_lat number(38,32))
RETURNS FLOAT AS
$$
st_distance(
    st_makepoint('-104.97300245114094','39.76471253574085')
    ,st_makepoint(loc_lng,loc_lat)
    )
$$;

CREATE OR REPLACE FUNCTION distance_to_mc(loc_lng number(38,32),loc_lat number(38,32))
RETURNS FLOAT
AS
$$
st_distance(
    st_makepoint('?','?')
    ,st_makepoint(loc_lng,loc_lat)
    )
$$
;

-- 🥋 Test the New Function!
--Tivoli Center into the variables 
set tc_lng='-105.00532059763648'; 
set tc_lat='39.74548137398218';

select distance_to_mc($tc_lng,$tc_lat);


-- 🎯 Convert the List into a View
CREATE OR REPLACE VIEW LOCATIONS.COMPETITION AS select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

-- which competitors are closest to Melanie's Cafe?
SELECT
 name
 ,cuisine
 , ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_to_melanies
 ,*
FROM  competition
ORDER by distance_to_melanies;

-- 📓 Why Not Use the UDF We Just Created? 
/* 
Since Sonra data stores coordinates as geoJSON GEOGRAPHY objects in a single column, our function,
which requires separate Latitude and Longitude inputs,
won't work directly. While we could parse the coordinates back into separate values and
pass them into the function,
a better solution is to create a function that accepts the GEOGRAPHY object directly */
-- 🥋 Changing the Function to Accept a GEOGRAPHY Argument 
-- We've highlighted the changed parts in blue.


CREATE OR REPLACE FUNCTION distance_to_mc(lng_and_lat GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lng_and_lat
        )
  $$
  ;

-- 🥋 Now We Can Use it In Our Sonra Select
  
SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_to_melanies
 ,*
FROM  competition
ORDER by distance_to_melanies;


/**

📓 What's Going On? FUNCTION OVERLOADING ON Locations schema

Initially, we had a DISTANCE_TO_MC function with two arguments.
After running a CREATE OR REPLACE statement to redefine it with just one argument,
you might expect only one version to remain. 
However, checking your LOCATIONS schema under FUNCTIONS, you'll find both versions still exist.
 */



-- 🥋 Different Options, Same Outcome!

-- Tattered Cover Bookstore McGregor Square
set tcb_lng='-104.9956203'; 
set tcb_lat='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lng,$tcb_lat);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lng,$tcb_lat));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

/* Analyze  Potential Promotion Partnerts*/
-- create view DENVER_BIKE_SHOPS as
-- select name
-- , ST_DISTANCE(

-- )AS distance_to_melanies
-- , coordinates
-- from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
-- where  shop ='bicycle'


create or replace view MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.DENVER_BIKE_SHOPS(
	NAME,
	DISTANCE_TO_MELANIES,
	COORDINATES
) as
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, coordinates
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
where shop='bicycle' ;



select * from DENVER_BIKE_SHOPS 
order by distance_to_melanies;


/* LESSON 9 */

--  this outputs errors
create or replace external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(100) as (metadata$filename::varchar(100))
) 
location= @trails_parquet
auto_refresh = true
file_format = (type = parquet);

/*
📓 Why Use External Storage for External Tables?
External tables are designed to work with external storage. Mel and Zena use them for rapid prototyping
without loading data. Organizations often avoid loading data into Snowflake to keep it denormalized,
maintain security, prevent multiple data copies, or avoid vendor lock-in.
Typically, externally stored data resides in Azure Blob, GCP Buckets, or AWS S3.
In our workshops, we simulate 
this process using an AWS S3 bucket so you can explore external tables without needing your own cloud account.
 */

/*After configuring an external stage this should work*/
create or replace external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(100) as (metadata$filename::varchar(100))
) 
location= @external_aws_dlkw
auto_refresh = true
file_format = (type = parquet);

select * from T_CHERRY_CREEK_TRAIL;

/*
You can create a Materialized View on an External Table, even if the table is based on staged data. 
This setup allows you to:

Correct data issues in the Parquet file (e.g., flipped Longitude and Latitude).
Calculate the distance to Melanie's Cafe for all 3500 points on the trail.
Using a Materialized View is efficient here because it avoids recalculating the distance every time, 
updating only when the underlying data or location changes. */


create secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as
select 
 value:sequence_1 as point_id,
 value:trail_name::varchar as trail_name,
 value:latitude::number(11,8) as lng,
 value:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair,
 locations.distance_to_mc(lng,lat) as distance_to_melanies
from t_cherry_creek_trail;

/*The syntax value:field_name is used in Snowflake to access fields within a semi-structured data type 
like JSON or VARIANT. 
This notation is often seen in queries involving JSON data or other semi-structured 
formats where fields are accessed using the colon (:) operator. */


-- 📓  Iceberg Tables, account admin role
use role accountadmin;
CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://uni-dlkw-iceberg'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::321463406630:role/dlkw_iceberg_role'
            STORAGE_AWS_EXTERNAL_ID = 'dlkw_iceberg_id'
         )
      );
 

 DESC EXTERNAL VOLUME iceberg_external_volume;


 -- 🎯 Create an Iceberg Database

create database my_iceberg_db
 catalog = 'SNOWFLAKE'
 external_volume = 'iceberg_external_volume';

 --🥋 Create a Table 

 set table_name = 'CCT_'||current_account();

create iceberg table identifier($table_name) (
    point_id number(10,0)
    , trail_name string
    , coord_pair string
    , distance_to_melanies decimal(20,10)
    , user_name string
)
  BASE_LOCATION = $table_name
  AS SELECT top 100
    point_id
    , trail_name
    , coord_pair
    , distance_to_melanies
    , current_user() -- to avoid overwriting in shared cloud environment
  FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;

-- 🎯 Query the Iceberg Table
select * from identifier($table_name); 

-- editing values
update identifier($table_name)
set user_name = 'I am amazing!!'
where point_id = 1;
