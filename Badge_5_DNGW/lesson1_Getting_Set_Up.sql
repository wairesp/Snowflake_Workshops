alter user <my user name> set default_role = 'SYSADMIN';
alter user <my user name> set default_warehouse = 'COMPUTE_WH';
alter user <my user name> set default_namespace = 'UTIL_DB.PUBLIC';

ALTER USER <my user name> SET DEFAULT_ROLE = 'SYSADMIN';


-- Create the Project Infrastructure
use role sysadmin;
CREATE DATABASE AGS_GAME_AUDIENCE;
DROP SCHEMA AGS_GAME_AUDIENCE.PUBLIC;
CREATE SCHEMA AGS_GAME_AUDIENCE.RAW;

use AGS_GAME_AUDIENCE.RAW;

create or replace TABLE AGS_GAME_AUDIENCE.RAW.GAME_LOGS (
	RAW_LOG VARIANT
);

truncate table AGS_GAME_AUDIENCE.RAW.GAME_LOGS;
-- 🥋 Load the File Into The Table
list @uni_kishore/kickoff;

select $1
from @uni_kishore/kickoff
(file_format => 'ff_json_logs');

copy into ags_game_audience.raw.game_logs
from @uni_kishore/kickoff -- attempt to load all the files in the stage when is not specified
file_format = (format_name = ff_json_logs);


-- 🥋 Build a Select Statement that Separates Every Attribute into It's Own Column

select
RAW_LOG:agent::text as AGENT
, RAW_LOG:user_event::text as USER_EVENT
, RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601
, RAW_LOG:user_login::text as user_login
from game_logs;

-- 📓 Wrapping Selects in Views 
-- 🎯 Create Your View, named logs in raw
use role sysadmin;
use schema ags_game_audience.raw;

CREATE VIEW LOGS AS select
RAW_LOG:agent::text as AGENT
, RAW_LOG:user_event::text as USER_EVENT
, RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601
, RAW_LOG:user_login::text as user_login
from game_logs;

select current_timestamp();


-- Change the timezone

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'America/Lima';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();


alter session set timezone = 'America/Denver';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';


-- Exploring the File Before Loading It
select
    $1
from
    @uni_kishore/updated_feed (file_format => FF_JSON_LOGS); -- Load the File Into The Table


-- Load the File Into The Table
copy into AGS_GAME_AUDIENCE.RAW.GAME_LOGS
from
    @uni_kishore/updated_feed 
    file_format =(format_name = FF_JSON_LOGS);

select * from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

create or replace view AGS_GAME_AUDIENCE.RAW.LOGS  as select
-- RAW_LOG:agent::text as AGENT,
RAW_LOG:user_event::text as USER_EVENT
, RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601
, RAW_LOG:user_login::text as user_login
, RAW_LOG:game_id::text as game_id
, RAW_LOG:ip_address::text as ip_address
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
WHERE ip_address is not null;

-- 🎯 CHALLENGE: Filter Out the Old Rows
select * from logs 
where agent is not null;


-- 📓 Filter Out the Old Records
-- Remember that the first set of records included the AGENT field,
--  but in the second set of records would have an empty AGENT value. 
-- The first set of records did NOT include IP_ADDRESS, but in the second set of records,
--  there should be an IP_ADDRESS. 

-- 🥋 Two Filtering Options

--looking for empty AGENT column
select * 
from ags_game_audience.raw.LOGS
where agent is null;

--looking for non-empty IP_ADDRESS column
select 
ip_address,
*
from LOGS
where ip_address::text is not null;


/* Testing updated LOGS */

select * from AGS_GAME_AUDIENCE.RAW.LOGS
ORDER BY USER_LOGIN ASC;

-- 📓 Kishore's Test Rows - His Sister's Gaming 
-- 🎯 Find Prajina's Log Events in Your Table
select * from AGS_GAME_AUDIENCE.RAW.LOGS
where user_login ILIKE '%kishore%';

