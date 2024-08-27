use role sysadmin;
create stage if not exists AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
url = 's3://uni-kishore-pipeline'
;

list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

-- Great news! We've already automated Step 1 for you. 
-- To keep things simple and avoid the need for you to set up an AWS account and bucket, 
-- we've simulated a production environment where files are delivered to your bucket every 5 minutes.
-- If you set up a stage and run a LIST command, you'll see new files being added regularly.

-- 🎯 Create A New Raw Table!
create or replace TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS (
	RAW_LOG VARIANT
);

-- IMPORTANT NOTE:  In line 2 of your COPY INTO command DO NOT include a folder or filename. Just put either: 

-- FROM @uni_kishore_pipeline , or
-- FROM @ags_game_audience.raw.uni_kishore_pipeline
-- and the command will pick up every available file and try to load it! You should NOT specify a file name. 
COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
file_format = (format_name = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);


truncate table PL_GAME_LOGS;


CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    warehouse='COMPUTE_WH'
    SCHEDULE='5 minute' -- inially 10 but changed advanzing in the lesson
    AS COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
    file_format = (format_name = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);


EXECUTE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;
select COUNT(*) from PL_GAME_LOGS;
-- CREATE A NEW JSON PARSING VIEW

CREATE VIEW AGS_GAME_AUDIENCE.RAW. PL_LOGS AS select
-- RAW_LOG:agent::text as AGENT,
RAW_LOG:user_event::text as USER_EVENT
, RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601
, RAW_LOG:user_login::text as user_login
, RAW_LOG:game_id::text as game_id
, RAW_LOG:ip_address::text as ip_address
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
WHERE ip_address is not null;


SELECT * FROM PL_LOGS;

LIST  @UNI_KISHORE_PIPELINE;

-- 🎯 Modify the Step 4 MERGE Task !

-- Files are now loaded from UNI_KISHORE_PIPELINE into PL_GAME_LOGS and parsed by PL_LOGS.
-- The destination table LOGS_ENHANCED and the merge task LOAD_LOGS_ENHANCED remain the same.
-- Update your merge code to reflect these changes and manually
-- run LOAD_LOGS_ENHANCED to ensure it inserts new rows into LOGS_ENHANCED.

create or replace task ags_game_audience.raw.LOAD_LOGS_ENHANCED
warehouse='COMPUTE_WH'
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
                from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs -- changed from LOGS to PL_LOGS
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

/*
📓 Allowing Our Task to Run Itself
Step 1: Files load into the external stage every 5 minutes, fully
automated with no compute costs, managed by Snowflake Education.

Step 2: A task set to run every 5 minutes is not yet automated. Using
the EXECUTE TASK command can quickly use up trial credits, especially
with a high-frequency task.

Ensure safeguards before scheduling tasks. Set up a Resource Monitor
to control pipeline costs.

📓 Forgotten Tasks Can Eat Up Credits, Fast!
A task set to run every 5 minutes, once activated, will use trial
credits. To prevent this, set up a RESOURCE MONITOR with a daily limit
of 1 credit hour. This will shut down tasks daily, avoiding credit waste.

🎯 Create a Resource Monitor to Limit Usage
We’ll create a daily Resource Monitor with a 1 credit hour limit. It
can be adjusted as needed to prevent runaway costs.
*/

-- Set Up A Resource Monitor
use role accountadmin;
create resource monitor if not exists daily_shut_down
with credit_quota = 1
frequency = daily
start_timestamp = immediately
triggers
    on 50 percent do notify
    on 75 percent do suspend
    on 98 percent do suspend_immediate
;


/*
🎯 Truncate The Target Table
Before testing the new pipeline, TRUNCATE the target table
ENHANCED.LOGS_ENHANCED to remove old rows. Starting with zero rows
makes it easier to verify that the new processes work as expected.
*/
use role sysadmin;

truncate table ENHANCED.LOGS_ENHANCED;

/*
📓 The Current State of Things
Our process is looking good. We have:

Step 1 TASK (invisible to you, but running every 5 minutes)
Step 2 TASK that will load the new files into the raw table every 5 minutes (as soon as we turn it on).
Step 3 VIEW that is kind of boring but it does some light transformation (JSON-parsing) work for us.  
Step 4 TASK  that will load the new rows into the enhanced table every 5 minutes (as soon as we turn it on).


So let's turn on the TASKS!! 

🥋 Turn on Your Tasks!
You can suspend and resume tasks using the GUI. 

You can also resume and suspend them using worksheet code. 
*/

--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

--Turning OFF a task is done with a SUSPEND command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

/*

❕ You Have Tasks Running!
You have tasks currently running and a Resource Monitor set to stop
everything after one hour.

Always shut off tasks when you finish for the day to save credits. The
Resource Monitor helps protect your free credits but turning off tasks
manually is more efficient.

If the Resource Monitor shuts everything down due to exceeding your
daily quota, adjust the quota to 2 or 3 credits in the monitor settings
to continue working. The monitor is there to protect you but can be
edited as needed.
 */


/*🥋 Let's Check Our Tasks
Go to the LOAD_LOGS_ENHANCED task page.
Verify if the task is owned by SYSADMIN and if it’s running. Note the
next scheduled run time if it’s SCHEDULED.
Refresh the page after it runs again and check if it succeeded.
NOTE: If the task isn’t owned by SYSADMIN, SUSPEND it, change the
ownership, and then RESUME it. If it’s not running, use the ALTER command
ending in RESUME.

🎯 Check on the GET_NEW_FILES Task
Use the same methods to check your other scheduled task. Ensure it’s
running and succeeding!
*/

show tasks;

use role accountadmin;
SELECT *
FROM snowflake.account_usage.task_history
ORDER BY completed_time DESC
LIMIT 100;

/*🏆 Keeping Tallies in Mind
Check Files: Count the files in the stage and multiply by 10. This
is your expected row count.

Check PL_GAME_LOGS: Verify the number of rows in PL_GAME_LOGS,
which is populated by the GET_NEW_FILES task.

Check PL_LOGS: The PL_LOGS view normalizes PL_GAME_LOGS without
altering the row count. Ensure PL_LOGS has the same number of rows.

Check LOGS_ENHANCED: Confirm the number of rows in LOGS_ENHANCED,
which is enhanced using PL_LOGS and other tables. No rows should be lost.

NOTE: Row loss in Step 4 may occur due to failed time zone lookups
against IPINFO_GEOLOC. This is acceptable at this project phase. */

-- 🥋 Checking Tallies Along the Way
--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

/*
📓 A Few Task Improvements
Timing Issues: Timing discrepancies might affect row counts. For example,
a file might be added but not picked up by GET_NEW_FILES, or rows might
be processed by GET_NEW_FILES but not by LOAD_LOGS_ENHANCED.

TASK DEPENDENCIES:
To address this, set up task dependencies. You can’t control the Step 1 task,
but you can manage the Step 2 (GET_NEW_FILES) and Step 4 (LOAD_LOGS_ENHANCED)
tasks. Consider running GET_NEW_FILES every 5 minutes and triggering
LOAD_LOGS_ENHANCED only after GET_NEW_FILES finishes, reducing uncertainty.

SERVERLESS COMPUTE:
Currently, your warehouse spins up for each task run and may not auto-suspend
in time, leading to high costs. Instead, use Snowflake’s "SERVERLESS" option,
which leverages existing compute resources and is more cost-effective for small,
frequent tasks. Grant the SERVERLESS privilege to SYSADMIN to use this mode.
 */

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;

-- 🥋 Replace the WAREHOUSE Property in Your Tasks
-- USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'

-- 🥋 Replace or Update the SCHEDULE Property
-- Use one of these lines in each task. Make sure you are using the SYSADMIN role when you replace these task definitions.  

-- --Change the SCHEDULE for GET_NEW_FILES so it runs more often
-- schedule='5 Minutes'

-- --Remove the SCHEDULE property and have LOAD_LOGS_ENHANCED run  
-- --each time GET_NEW_FILES completes
-- after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
create or replace task ags_game_audience.raw.LOAD_LOGS_ENHANCED
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
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
                from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs -- changed from LOGS to PL_LOGS
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

-- 🎯 Resume the Tasks
-- After editing a task, remember to RESUME it using either the GUI or
-- an ALTER command.

-- For tasks with dependencies, resume the dependent tasks before the
-- triggering tasks. First, resume LOAD_LOGS_ENHANCED, then resume GET_NEW_FILES.

-- FYI: The first task in the chain is called the Root Task. In this case,
-- GET_NEW_FILES is the Root Task.

alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;

execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Once a task finishes, it triggers the next one. The DORA check will pass
-- only after all tasks complete. Ensure all tasks succeed before the DORA check.


 /*
 🎯 Allow Your Tasks to Succeed, Then Suspend The Root
Once you've seen the new versions of the tasks succeed and passed the DORA check above, you can SUSPEND the root task.  

If you are stopping your learning for today, suspend your root task until you need it again.

REMEMBER: It is your responsibility to protect your free trial credits. If you squander your credits and run out before completing the badge requirements, you will have to start over with a new trial account, or enter a credit card to finish the workshop.  This is NOT the same as exceeding the quota on a resource monitor. Resource monitors are easy to reset and fully in your control. 
*/

select max(tally) from (
    select 
        CASE WHEN SCHEDULED_FROM = 'SCHEDULE' and STATE= 'SUCCEEDED' THEN 1 ELSE 0 END as tally 
        ,SCHEDULED_FROM
        ,STATE
        ,scheduled_time
        ,*
    from table(ags_game_audience.information_schema.task_history (task_name=>'GET_NEW_FILES'))
)
;