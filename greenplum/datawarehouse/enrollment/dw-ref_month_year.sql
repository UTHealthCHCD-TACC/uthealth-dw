drop table data_warehouse.ref_month_year;

create table data_warehouse.ref_month_year as	
select substring( replace(datum::text,'-',''),1,6)::int4 AS month_year_id,
       datum AS start_of_month,
       ( datum + interval '1 month' - interval '1 day' )::date as end_of_month,
       ( datum + interval '1 month' - interval '1 day' )::date - datum + 1 as days_in_month,
       EXTRACT(MONTH FROM datum) AS month_int,
       TO_CHAR(datum,'Month') AS month_name
FROM (	  
select date(datum) as datum
      FROM GENERATE_SERIES ('2007-01-01'::DATE, '2022-12-31'::DATE, '1 month') AS datum     
) DQ
ORDER BY 1;

-- Add Columns
alter table data_warehouse.ref_month_year add column year_int smallint;
update data_warehouse.ref_month_year set year_int = extract(year from start_of_month);

--UT FY
--NOTE: Doesn't handle 00->99
alter table data_warehouse.ref_month_year add column fy_ut char(2);
alter table data_warehouse.ref_month_year add column fy_ut_temp int;
update data_warehouse.ref_month_year set fy_ut_temp = cast(substring(cast(year_int as varchar), 3) as int); 
update data_warehouse.ref_month_year set fy_ut = case 
											when month_int >= 9 then LPAD(cast(cast(fy_ut_temp+1 as int) as varchar), 2, '0') 
											else LPAD(cast(cast(fy_ut_temp as int) as varchar), 2, '0')
										end;
alter table data_warehouse.ref_month_year drop column fy_ut_temp;									
select * from data_warehouse.ref_month_year;


select * from data_warehouse.ref_month_year;





