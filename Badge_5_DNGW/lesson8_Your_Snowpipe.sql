/*
Now that an Simple Notification System(SNS) Topic and Bucket event notification 
are set up (thanks to the Snowflake Ed Services team),
you can proceed with the CREATE PIPE step!

Each of you will use the same CREATE PIPE statement with the same SNS 
Topic value. Voilà! You'll have a working Snowpipe!
 */

-- 🥋 Create Your Snowpipe!


CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

/*Our Event-Driven Pipeline Progress

We have one more step to complete our Event-Driven Pipeline: updating the 
`LOAD_LOGS_ENHANCED` task to load from the `ED_PIPELINE_LOGS` table instead 
of `PIPELINE_LOGS`. */


-- 🎯 Update the LOAD_LOGS_ENHANCED Task
/* 1. **Truncate** the `LOGS_ENHANCED` table to avoid confusion with previous 
   pipeline results. Optionally, create a backup table (`LOGS_ENHANCED_BACKUP`).

2. **Edit** the `LOAD_LOGS_ENHANCED` task to load from `ED_PIPELINE_LOGS` 
   instead of `PL_LOGS`. Suspend the task if it's running.

3. **Schedule** the task to run every 5 minutes instead of as a triggered 
   task. Resume the task.

   > **Note:** Ensure you have both one PIPE and one TASK running. Adjust 
   > your Resource Monitor to allow 2 or 3 credit hours if needed. */

truncate ags_game_audience.enhanced.logs_enhanced;
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;
create or replace task ags_game_audience.raw.LOAD_LOGS_ENHANCED
warehouse=COMPUTE_WH
schedule='5 minute'
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
                from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS logs -- changed from PL_LOGS to ED_PIPELINE_LOGS
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
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

-- ***************************************************************
-- Use this command if your Snowpipe seems like it is stalled out:

ALTER PIPE ags_game_audience.raw.PIPE_GET_NEW_FILES REFRESH;
-- ***************************************************************
-- Use this command if you want to check that your pipe is running:

select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));

/*📓 Fully Event-Driven?
Our pipeline isn't fully event-driven yet. Here's the breakdown:

STEP 1: Time-Driven
Reason: The bucket gets files at regular intervals because it's simulated.
Real World: In practice, logs would arrive at varying rates as gamers log in.
STEP 2: Event-Driven
Status: This step is fully event-driven.
STEP 3: Time-Driven
Issue: This step remains time-driven.
Good News: You now have hands-on experience with Snowflake’s tasks and pipes.
Enhancing STEP 3: Adding a STREAM
STREAMS can be complex, but we'll start with a basic one.
Purpose: A STREAM won't replace the last task but will improve efficiency.
Technique: It will use "Change Data Capture" (CDC) to streamline the pipeline.
*/

-- 🥋 Create a Stream


--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--look at the stream you created
show streams;

--check to see if any changes are pending (expect FALSE the first time you run it)
--after the Snowpipe loads a new file, expect to see TRUE
select system$stream_has_data('ed_cdc_stream');


/*🎯 Suspend the LOAD_LOGS_ENHANCED Task
The LOAD_LOGS_ENHANCED task scans every row to check if it’s been added
to the LOGS_ENHANCED table. This method is inefficient, especially if the
task runs over a long period, wasting compute power.

We’ll create a new MERGE task that is more efficient. For now, SUSPEND
the LOAD_LOGS_ENHANCED task—it won’t be needed again.*/

-- 🥋 Suspend the LOAD_LOGS_ENHANCED Tas
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

/*📓 Streams Can Be VERY Complex - Ours is Simple
Streams can be quite complex, involving offsets, data retention, staleness,
and various types. For now, we'll use the simplest form of a stream.

In our case, it will only handle record inserts and won't track or process
every change. We’re focusing on the most basic use for this workshop. */


-- 🥋 View Our Stream Data

--query the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('PIPE_GET_NEW_FILES');

--if you need to pause or unpause your pipe
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = false;

-- 📓 Processing Our Simple Stream
-- We'll use the records in our STREAM to insert new records into the LOGS_ENHANCED table.
--  This is more sophisticated than using a MERGE that looks at EVERY row in our source table. 

-- 🥋 Process the Rows from the Stream

--make a note of how many rows are in the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

 
--process the stream by using the rows in a merge 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as GAME_EVENT_LTZ
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
 
--Did all the rows from the stream disappear? 
select * 
from ags_game_audience.raw.ed_cdc_stream; 


/*📓 What Happens if the Merge Fails?
Streams can be complex due to their handling of disappearing information. In
production systems, you'd typically implement safeguards to avoid losing data.
For instance, you might run an older task once daily to catch anything missed
by the stream processing task.

This example is simplified, but it demonstrates how streams can be powerful in
Change Data Capture pipelines. */

/*
📓 The Final Task in Our Pipeline - Ripe For Improvement
With the PIPE and STREAM in place, we just need a task at the end that pulls
new data from the STREAM, rather than from the RAW data table. We can use the
MERGE statement we just tested. */

-- 🥋 Create a CDC-Fueled, Time-Driven Task



--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;


/*
📓 A Final Improvement!
Let’s add a final enhancement to our pipeline.

This won’t change load costs, as files load every 5 minutes by design,
but it will refine the last step of the pipeline.

We’ll add a WHEN clause to the task that checks the stream. The task will
still run every 5 minutes, but if no changes are detected, it will skip
running. 
*/

-- 🎯 Add A Stream Dependency to the Task Schedule
-- Add STREAM dependency logic to the TASK header and replace the task. 

CREATE OR REPLACE TASK ags_game_audience.raw.cdc_load_logs_enhanced
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
  SCHEDULE = '5 minutes'
WHEN
  system$stream_has_data('ed_cdc_stream')
AS 
  MERGE INTO ags_game_audience.enhanced.logs_enhanced ASe
  USING (
    SELECT
      cdc.ip_address,
      cdc.user_login AS gamer_name,
      cdc.user_event AS game_event_name,
      cdc.datetime_iso8601 AS game_event_utc,
      city,
      region,
      country,
      timezone AS gamer_ltz_name,
      CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) AS game_event_ltz,
      DAYNAME(game_event_ltz) AS dow_name,
      tod_name
    FROM ags_game_audience.raw.ed_cdc_stream AScdc
    INNER JOIN ipinfo_geoloc.demo.location ASloc 
      ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
      AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
      BETWEEN start_ip_int AND end_ip_int
    INNER JOIN ags_game_audience.raw.time_of_day_lu AStod
      ON HOUR(game_event_ltz) = tod.hour
  ) ASr
    ON r.gamer_name = e.gamer_name
    AND r.game_event_utc = e.game_event_utc
    AND r.game_event_name = e.game_event_name 
  WHEN NOT MATCHED THEN 
    INSERT (
      ip_address, gamer_name, game_event_name,
      game_event_utc, city, region,
      country, gamer_ltz_name, game_event_ltz,
      dow_name, tod_name
    )
    VALUES
    (
      ip_address, gamer_name, game_event_name,
      game_event_utc, city, region,
      country, gamer_ltz_name, game_event_ltz,
      dow_name, tod_name
    );
        
--Resume the task so it is running
ALTER TASK ags_game_audience.raw.cdc_load_logs_enhanced RESUME;

/*📓 Confirming Data is Flowing

The data makes two stops in your account. The first table is
ED_PIPELINE_LOGS. The data lands here because of the Snowpipe named
GET_NEW_FILES.

We can see that this table has 2.3K records now (your count may vary
depending on the time of day) and is loading every 5 minutes as files
arrive in the bucket.

If we refresh this page, the number of rows should increase by 10 every
five minutes.

The second stop for the data is the AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED table.

Use all available tools to check that both the pipe and task are working
to move data from the stage to the enhanced table. Remember, there should
be 10 records in each new file, but not all will reach the Enhanced table
due to the join with the IPInfo share table. */

SELECT *
FROM ags_game_audience.raw.ed_pipeline_logs
;

SELECT *
FROM ags_game_audience.enhanced.logs_enhanced
;

