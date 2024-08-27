select $1
from @sweatsuits/purple_sweatsuit.png; 

/** 
She wants Snowflake to give her list of files not the insides of each file,
separated into multiple rows. **/

select metadata$filename, COUNT(metadata$file_row_number)
from @sweatsuits/purple_sweatsuit.png
group by metadata$filename;

-- 🥋 Query the Directory Table of a Stage
/*We've also seen how Unstructured data, not loaded into Snowflake, 
can be accessed with a special Snowflake tool called a Directory Table. */
select * 
from directory(@sweatsuits);


-- 🥋 Start By Checking Whether Functions will Work on Directory Tables
select REPLACE(relative_path, '_', ' ') as no_underscores_filename
, REPLACE(no_underscores_filename, '.png') as just_words_filename
, INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

-- 📓 Cool Snowflake SQL Trick!
/* Zena was able to define a column using the AS syntax, 
and then use that column name in the very next line of the same SELECT? 
This is not true in many other database systems */
select REPLACE (relative_path, '_', ' ') as no_underscores_filename
, REPLACE( no_underscores_filename,'.png') as just_words_filename
, INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

-- Nest 3 Functions into 1 Statement
/*She knows she could NEST the functions instead of creating 3 columns on her way to her goal.
 Now that she's tested things, she's going to NEST the functions progressively. */
select REPLACE(REPLACE(relative_path, '_', ' '), '.png') as just_words_filename
, INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

select INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png')) as product_name
from directory(@sweatsuits);

-- JOIN
--create an internal table for some sweatsuit info
create or replace table zenas_athleisure_db.products.sweatsuits (
	color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);

--fill the new table with some data
insert into  zenas_athleisure_db.products.sweatsuits 
          (color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'forest_green_sweatsuit.png',64)
,('Navy Blue', 'navy_blue_sweatsuit.png',65)
,('Orange', 'orange_sweatsuit.png',65)
,('Pink', 'pink_sweatsuit.png',63)
,('Purple', 'purple_sweatsuit.png',64)
,('Red', 'red_sweatsuit.png',68)
,('Royal Blue',	'royal_blue_sweatsuit.png',65)
,('Yellow', 'yellow_sweatsuit.png',67);


create or replace view zenas_athleisure_db.products.product_list as select initcap(replace(replace(relative_path, '_', ' '), '.png')) as product_name,
file_name, color_or_style, price, file_url
from directory(@sweatsuits) as d
join sweatsuits as s
on d.relative_path = s.file_name;
-- adding a cross join
create or replace view catalog as select * 
from product_list p
cross join sweatsuit_sizes;

-- 🥋 Add the Upsell Table and Populate It


-- Add a table to map the sweatsuits to the sweat band sets
create table zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style varchar(25)
,upsell_product_code varchar(10)
);

--populate the upsell table
insert into zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style
,upsell_product_code 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

-- 🥋 Zena's View for the Athleisure Web Catalog Prototype
-- Zena needs a single view she can query for her website prototype
create view catalog_for_website as 
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, file_name
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code;
