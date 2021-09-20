/* ******************************************************************************************************
 *  This reference table is used by the load scripts for enrollment.
 *  Ensure there are dates far enough into the future.
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 */

drop table if exists reference_tables.ref_month_year;

create table reference_tables.ref_month_year
( month_year_id int4, prior_month_year_id int4, next_month_year_id int4, start_of_month date, end_of_month date, days_in_month int2,
  month_int int2, month_name text, year_int int2, fy_ut int2, my_row_counter int2)
distributed replicated;
											

insert into reference_tables.ref_month_year ( month_year_id, prior_month_year_id, next_month_year_id, start_of_month, end_of_month, days_in_month, month_int, month_name, year_int, fy_ut)	
select substring( replace(datum::text,'-',''),1,6)::int4 AS month_year_id,
       substring( replace((datum - interval '1 month')::text,'-',''),1,6)::int4,
       substring( replace((datum + interval '1 month')::text,'-',''),1,6)::int4,
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




update reference_tables.ref_month_year a  
set my_row_counter = sub.rn 
from  (
select row_number() over(order by month_year_id) as rn
      ,*
from reference_tables.ref_month_year 
) sub
where sub.month_year_id = a.month_year_id 


analyze reference_tables.ref_month_year;

select * from reference_tables.ref_month_year;



