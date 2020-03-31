drop table if exists reference_tables.ref_month_year;


create table reference_tables.ref_month_year
( month_year_id int4, start_of_month date, end_of_month date, days_in_month int2,
  month_int int2, month_name text, year_int int2, fy_ut int2)
distributed replicated;
											

insert into reference_tables.ref_month_year ( month_year_id, start_of_month, end_of_month, days_in_month, month_int, month_name, year_int, fy_ut)	
select substring( replace(datum::text,'-',''),1,6)::int4 AS month_year_id,
       datum AS start_of_month,
       ( datum + interval '1 month' - interval '1 day' )::date as end_of_month,
       ( datum + interval '1 month' - interval '1 day' )::date - datum + 1 as days_in_month,
       EXTRACT(MONTH FROM datum) AS month_int,
       TO_CHAR(datum,'Month') AS month_name,
       extract(year from datum) as year_int,
       case when EXTRACT(MONTH FROM datum) >= 9 then extract(year from datum)+1
											else extract(year from datum)
										end
FROM (	  
select date(datum) as datum
      FROM GENERATE_SERIES ('2007-01-01'::DATE, '2022-12-31'::DATE, '1 month') AS datum     
) DQ
ORDER BY 1;



select * from reference_tables.ref_month_year;





