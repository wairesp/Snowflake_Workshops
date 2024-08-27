alter database GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI 
rename to WEATHERSOURCE;

-- what countries are in the data
select distinct  country
from weathersource.standard_tile.history_day ;

-- does acme have the costal codes acme wants
select distinct  postal_code
from weathersource.standard_tile.history_day 
where country = 'US' and postal_code like '482%' OR postal_code like '481%';


-- how do we filter our data just to detroit
CREATE DATABASE MARKETING;
USE DATABASE MARKETING;
CREATE SCHEMA MAILERS;

CREATE OR REPLACE VIEW MAILERS.DETROIT_ZIPS AS
select distinct  postal_code
from weathersource.standard_tile.history_day 
where country = 'US' and postal_code like '482%' OR postal_code like '481%';


-- filter the dataand how much can i remove with a filter 

SELECT COUNT(*)
FROM weathersource.standard_tile.history_day;

SELECT COUNT(*)
FROM weathersource.standard_tile.history_day
INNER JOIN MAILERS.DETROIT_ZIPS 
ON MAILERS.DETROIT_ZIPS.postal_code = weathersource.standard_tile.history_day.postal_code;

-- what time frame are we working with

SELECT max(date_valid_std), min(date_valid_std)
from weathersource.standard_tile.forecast_day AS fd
JOIN marketing.mailers.detroit_zips AS dz
on fd.postal_code = dz.postal_code;

-- what days in the next few weeks?

SELECT date_valid_std, AVG(avg_cloud_cover_tot_pct)
FROM weathersource.standard_tile.forecast_day AS fd
JOIN marketing.mailers.detroit_zips AS dz
ON fd.postal_code = dz.postal_code
GROUP BY date_valid_std
ORDER BY AVG(avg_cloud_cover_tot_pct);