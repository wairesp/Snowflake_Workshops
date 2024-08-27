create database MELS_SMOOTHIE_CHALLENGE_DB;
drop schema MELS_SMOOTHIE_CHALLENGE_DB.public;

create schema trails;

create stage trails_geojson;

create stage trails_parquet;


-- Query Your TRAILS_GEOJSON Stage!
select *
from @trails_geojson
(file_format => ff_json);

-- Query Your TRAILS_PARQUET Stage!

select * 
from @trails_parquet
(file_format => ff_parquet);

-- LESSON 6
-- 🥋 Look at the Parquet Data

select 
    $1:sequence_1 as sequence_1,
    $1:trail_name::varchar as trail_name,
    $1:latitude::float as latitude,
    $1:longitude::varchar as longitude,
    $1:sequence_2::varchar as sequence_2,
    $1:elevation::float as elevation
from @trails_parquet
(file_format => ff_parquet)
order by sequence_1;

/* 🥋 Fix Coordinate Precision with a SELECT Statement
You only need up to 8 decimal places for millimeter accuracy in coordinates. 
Latitudes range from 0 to 90 (2 digits left of the decimal), and longitudes from 0 to 180 (3 digits).
Casting them as NUMBER(11,8) ensures precision. Here's the code to do that. */


--Nicely formatted trail data
use rol sysadmin;

create view CHERRY_CREEK_TRAIL as select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id;
--  🥋 Use || to Chain Lat and Lng Together into Coordinate Sets!
select top 100 
 lng||' '||lat as coord_pair
,'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

--To add a column, we have to replace the entire view
--changes to the original are shown in red
create or replace view cherry_creek_trail as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

-- linestring
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

-- 🎯 Can You Make The Whole Trail into a Single LINESTRING? 
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 2450
group by trail_name;

--  Normalize the Data Without Loading It!

select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

-- 🥋 Look at the geoJSON Data

select $1
from @trails_geojson
(file_format => ff_json);
-- 🥋 Normalize the Data Without Loading It!
create or replace view DENVER_AREA_TRAILS(
    FEATURE_NAME,
    FEATURE_COORDINATES,
    GEOMETRY,
    TRAIL_LENGTH,
    FEATURE_PROPERTIES,
    SPECS,
    WHOLE_OBJECT
) as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,st_length(to_geography(geometry))as trail_length
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

-- Lesson 7
-- Expore GeoSpatial Functions
--Remember this code? 
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
,to_geography(my_linestring) as length_of_trail --this line is new! but it won't work!
from cherry_creek_trail
group by trail_name;

-- 🎯 Calculate the Lengths for the Other Trails
select feature_name,
st_length(to_geography(geometry)) as wo_length,
st_length(to_geography(geometry)) as geom_lenght
from denver_area_trails
order by wo_length;

select get_ddl('view', 'DENVER_AREA_TRAILS');

select *
from denver_area_trails;
-- 🎯 Change your DENVER_AREA_TRAILS view to include a Length Column!

-- 🥋 Create a View on Cherry Creek Data to Mimic the Other Trail Data
--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create or replace view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry))  as trail_length
from cherry_creek_trail
group by trail_name;

-- 🥋 Use A Union All to Bring the Rows Into a Single Result Set
--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;

-- 🥋 Create a View on Cherry Creek Data to Mimic the Other Trail Data

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create or replace view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry))  as trail_length
from cherry_creek_trail
group by trail_name;

-- 🥋 Use A Union All to Bring the Rows Into a Single Result Set

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;


--Add more GeoSpatial Calculations to get more GeoSpecial Information! 
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

-- 🥋 Make it a View
create view trails_and_boundaries as
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

-- A Polygon Can be Used to Create a Bounding Box
select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;

