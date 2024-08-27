-- 🎯 Turn Things Off

-- Turn off any running tasks and pause your pipe with:
-- alter pipe mypipe set pipe_execution_paused = true;
-- We won't use these tasks or pipes anymore in this course.
ALTER TASK ags_game_audience.raw.cdc_load_logs_enhanced SUSPEND;

show pipes;

alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
-- 📓 Create a CURATED Layer

-- Once data is enhanced, a Data Engineer might move it to a Curated state.
-- This involves additional processing for better analysis and efficiency.

-- In this lesson, we'll create dashboard charts to check data quality and
-- use a windowing function to summarize data into a more meaningful set.

-- 🎯 Create a CURATED Layer

-- Create a SCHEMA named CURATED in the AGS_GAME_AUDIENCE database.
-- Ensure the schema is owned by SYSADMIN.

CREATE SCHEMA IF NOT EXISTS ags_game_audience.curated;

/*📓 Snowflake Dashboards

Snowflake dashboards can display charts and tables together.
They're not as advanced as Tableau or Looker but offer basic data analysis.
Though still new, Snowflake plans to improve dashboards in future releases.

Kishore's goal was to load the data for Agnie to analyze.
Before handing it off, he wants to create simple visualizations for basic insights. */

/*📓 What Matters Most?

Kishore reviews the ENHANCED_LOGS charts and considers what Agnie might value.
He notices duplicate events for each gamer and thinks the "game_session_length"
could be more valuable than individual login and logout data.

He wants to analyze if the time of day correlates with session length.
Although Agnie didn't request it, Kishore spends 30 minutes exploring this. */

--  🥋 Rolling Up Login and Logout Events with ListAgg



--the ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;

/*This is a quick way to aggregate rows, but to compare login and logout
times and measure game session lengths, we need a more sophisticated
method.

 */
--  🥋 Windowed Data for Calculating Time in Game Per Player

select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;


--  🥋 Code for the Heatgrid

--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF)
where logout is not null;

