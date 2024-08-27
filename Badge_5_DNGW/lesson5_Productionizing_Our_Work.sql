-- Create Simple Task
create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as SELECT 'hello';

use role accountadmin;
--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin; 

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- running task 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Run the task a few times to see changes in the RUN HISTORY
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    warehouse=COMPUTE_WH
    schedule='5 minute'
    as INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
    SELECT logs.ip_address
                , logs.user_login as GAMER_NAME
                , logs.user_event as GAME_EVENT_NAME
                , logs.datetime_iso8601 as GAME_EVENT_UTC
                , city
                , region
                , country
                , CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as GAMER_LTZ_NAME
                , DAYNAME(GAMER_LTZ_NAME) DOW_NAME 
                , lu.TOD_NAME
                from AGS_GAME_AUDIENCE.RAW.LOGS logs
                JOIN IPINFO_GEOLOC.demo.location loc 
                ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
                AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
                BETWEEN start_ip_int AND end_ip_int
                JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU lu ON EXTRACT(HOUR FROM GAMER_LTZ_NAME) = lu.hour;

--make a note of how many rows you have in the table
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any!)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- 📓 Dump And Refresh - A Y2K Party!
-- 🥋 Create a Backup Copy of the Table
--clone the table to save this version as a backup
--since it holds the records from the UPDATED FEED file, we'll name it _UF

drop table if exists AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF;
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;
--  Sophisticated 2010's - The Merge!
-- this is an update merge
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
ON r.user_login = e.GAMER_NAME
AND r.datetime_iso8601 = e.GAME_EVENT_UTC -- this was enought to make a valid match
AND r.user_event = e.GAME_EVENT_NAME
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';

-- If we had not used the CLONE feature to make a copy of the table,
-- we could use TIME TRAVEL to go back to the table right before we wiped out all the IP_ADDRESS values. 
-- Check out the documentation for TIME TRAVEL if you are interested in learning more. 

-- 🥋 Build Your Insert Merge
USE DATABASE ags_game_audience;
MERGE INTO enhanced.logs_enhanced AS e
USING (
   SELECT logs.ip_address
                , logs.user_login as GAMER_NAME
                , logs.user_event as GAME_EVENT_NAME
                , logs.datetime_iso8601 as GAME_EVENT_UTC
                , city
                , region
                , country
                , CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as GAMER_LTZ_NAME
                , DAYNAME(GAMER_LTZ_NAME) DOW_NAME 
                , lu.TOD_NAME
                from AGS_GAME_AUDIENCE.RAW.LOGS logs
                JOIN IPINFO_GEOLOC.demo.location loc 
                ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
                AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
                BETWEEN start_ip_int AND end_ip_int
                JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU lu ON EXTRACT(HOUR FROM GAMER_LTZ_NAME) = lu.hour
) AS r
  ON r.gamer_name = e.gamer_name
  AND r.game_event_utc = e.game_event_utc
  AND r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
  INSERT (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC, 
    CITY, 
    REGION, 
    COUNTRY, 
    GAMER_LTZ_NAME,
    DOW_NAME, 
    TOD_NAME
  )
  VALUES (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC, 
    CITY, 
    REGION, 
    COUNTRY, 
    GAMER_LTZ_NAME,
    DOW_NAME, 
    TOD_NAME
  )
;

truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


-- 📓 One Bite at a Time

CREATE OR REPLACE TASK ags_game_audience.raw.load_logs_enhanced
  warehouse='COMPUTE_WH'
  SCHEDULE='5 minute'
AS MERGE INTO enhanced.logs_enhanced AS e
USING (
   SELECT logs.ip_address
                , logs.user_login as GAMER_NAME
                , logs.user_event as GAME_EVENT_NAME
                , logs.datetime_iso8601 as GAME_EVENT_UTC
                , city
                , region
                , country
                , CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) as GAMER_LTZ_NAME
                , DAYNAME(GAMER_LTZ_NAME) DOW_NAME 
                , lu.TOD_NAME
                from AGS_GAME_AUDIENCE.RAW.LOGS logs
                JOIN IPINFO_GEOLOC.demo.location loc 
                ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
                AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
                BETWEEN start_ip_int AND end_ip_int
                JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU lu ON EXTRACT(HOUR FROM GAMER_LTZ_NAME) = lu.hour
) AS r
  ON r.gamer_name = e.gamer_name
  AND r.game_event_utc = e.game_event_utc
  AND r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
  INSERT (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC, 
    CITY, 
    REGION, 
    COUNTRY, 
    GAMER_LTZ_NAME,
    DOW_NAME, 
    TOD_NAME
  )
  VALUES (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC, 
    CITY, 
    REGION, 
    COUNTRY, 
    GAMER_LTZ_NAME,
    DOW_NAME, 
    TOD_NAME
  )
;

EXECUTE TASK ags_game_audience.raw.load_logs_enhanced;
-- 🥋 Testing Cycle (Optional)
--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 


