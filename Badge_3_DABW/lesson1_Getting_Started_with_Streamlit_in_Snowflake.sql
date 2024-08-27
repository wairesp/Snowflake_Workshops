use role sysadmin;
CREATE DATABASE IF NOT EXISTS SMOOTHIES;

create table PUBLIC.FRUIT_OPTIONS (
    FRUIT_ID INTEGER,
    NAME VARCHAR(25)
);

ALTER TABLE PUBLIC.FRUIT_OPTIONS
RENAME COLUMN NAME TO FRUIT_NAME;


create file format smoothies.public.two_headerrow_pct_delim
   type = CSV,
   skip_header = 2,   
   field_delimiter = '%',
   trim_space = TRUE
;

COPY INTO smoothies.public.fruit_options
FROM @smoothies.public.my_uploaded_files
files = ('fruits_available_for_smoothies.txt')
file_format = (format_name = smoothies.public.two_headerrow_pct_delim)
on_error = 'abort_statement'
validation_mode = 'RETURN_ERRORS'
purge = TRUE;

list @smoothies.public.my_uploaded_files;

-- QUERY TO STAGE
SELECT $1, $2 
FROM @my_uploaded_files/fruits_available_for_smoothies.txt
(FILE_FORMAT => smoothies.public.two_headerrow_pct_delim);

-- COPY INTO TABLE FIXED, 
-- CAUSED ERROR DUE TO ID NOT BEING THE FIRST COLUMN

TRUNCATE TABLE smoothies.public.fruit_options;
COPY INTO smoothies.public.fruit_options
FROM( SELECT $2 AS FRUIT_ID, $1 AS FRUIT_NAME
FROM @my_uploaded_files/fruits_available_for_smoothies.txt )
file_format = (format_name = smoothies.public.two_headerrow_pct_delim)
on_error = 'abort_statement'
purge = TRUE; -- Deletes the file after loading

SELECT * FROM smoothies.public.fruit_options;

create table smoothies.public.ORDERS (
    INGREDIENTS VARCHAR(200)
);

truncate table orders;

select * from smoothies.public.orders;

alter table ORDERS add column NAME_ON_ORDER varchar(100);

select * from smoothies.public.orders
where name_on_order is not null;

insert into smoothies.public.orders(ingredients, NAME_ON_ORDER) 
values ('Blueberries Dragon Fruit Elderberries Guava Jackfruit ','Naruto' );

ALTER TABLE ORDERS
ADD COLUMN ORDER_FILLED BOOLEAN DEFAULT FALSE;


ALTER TABLE ORDERS
DROP COLUMN ORDER_FILLED;

update smoothies.public.orders
set order_filled = true
where name_on_order is null;

/*Lesson 5: Ensuring Uniqueness */

-- TRUNCATE ORDERS FOR SQNCE
TRUNCATE TABLE smoothies.public.orders;

-- ADD THE UNIQUE ID COLUMN
alter table SMOOTHIES.PUBLIC.ORDERS 
add column order_uid integer --adds the column
default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column

drop table smoothies.public.orders;
create or replace table smoothies.public.orders (
       order_uid integer default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       constraint order_uid unique (order_uid),
       order_ts timestamp_ltz default current_timestamp()
);


set mystery_bag = 'What is in here?';

select $mystery_bag;

set var1 =2;    
set var2 =6;
set var3 =7;

select $var1 + $var2 + $var3;


use database util_db;
create or replace function public.sum_mystery_bag_vars (var1 number, var2 number, var3 number)
returns number as 'select var1+var2+var3';

DROP FUNCTION sum_mystery_bag_vars(NUMBER, NUMBER, NUMBER);
set eeny = 4;
set meeny = 67;
set miney_mo = -39;

select sum_mystery_bag_vars($eeny, $meeny, $miney_mo);


-- ADD THE SEARCH ON COLUMN
alter table SMOOTHIES.PUBLIC.fruit_options 
add column SEARCH_ON VARCHAR(100);

select * from smoothies.public.fruit_options;

UPDATE smoothies.public.FRUIT_OPTIONS
SET SEARCH_ON = FRUIT_NAME
WHERE SEARCH_ON IS NULL;
-- https://fruityvice.com/api/fruit/all
UPDATE smoothies.public.FRUIT_OPTIONS
SET SEARCH_ON = 'Fig'
WHERE FRUIT_NAME = 'Figs';
-- Apples, Blueberries, Jack Fruit, Raspberries and Strawberries
