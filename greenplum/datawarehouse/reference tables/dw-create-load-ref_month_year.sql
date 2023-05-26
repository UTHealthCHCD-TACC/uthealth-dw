/* ******************************************************************************************************
 *  This reference table is used by the load scripts for enrollment.
 *  Ensure there are dates far enough into the future.
 * ******************************************************************************************************
 *  Author || Date       || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021  || script created 
 * ******************************************************************************************************
 *  xzhang || 05/25/2023 || added backing up, updating the update log
 * 							added cy_end and fy_end to calculate ages
 * 							changed distributed replicated b/c it doesn't need to be there?
 * 							also ran this table out to 2030 bc why not
 */

--back up current table in case anything bad happens
drop table if exists backup.ref_month_year;

create table backup.ref_month_year as
select * from reference_tables.ref_month_year
distributed by (month_year_id);

--drop and create new reference table
drop table if exists reference_tables.ref_month_year;

create table reference_tables.ref_month_year
( month_year_id int4, prior_month_year_id int4, next_month_year_id int4, start_of_month date, end_of_month date, days_in_month int2,
  month_int int2, month_name text, year_int int2, fy_ut int2, cy_end date, fy_end date, my_row_counter int2)
distributed by (month_year_id);
											
insert into reference_tables.ref_month_year ( month_year_id, prior_month_year_id, next_month_year_id,
	start_of_month, end_of_month, days_in_month, month_int, month_name, year_int, fy_ut)	
select substring( replace(datum::text,'-',''),1,6)::int4 AS month_year_id,
       substring( replace((datum - interval '1 month')::text,'-',''),1,6)::int4 as prior_month_year_id,
       substring( replace((datum + interval '1 month')::text,'-',''),1,6)::int4 as next_month_year_id,
       datum AS start_of_month,
       ( datum + interval '1 month' - interval '1 day' )::date as end_of_month,
       ( datum + interval '1 month' - interval '1 day' )::date - datum + 1 as days_in_month,
       EXTRACT(MONTH FROM datum) AS month_int,
       TO_CHAR(datum,'Month') AS month_name,
       extract(year from datum) as year_int,
       case when EXTRACT(MONTH FROM datum) >= 9 then extract(year from datum)+1
			else extract(year from datum) end as fy_ut
FROM (	  
select date(datum) as datum
      FROM GENERATE_SERIES ('2007-01-01'::DATE, '2030-12-31'::DATE, '1 month') AS datum     
) DQ
ORDER BY 1;

--update cy_end, fy_end
update reference_tables.ref_month_year
set cy_end = ('12-31-' || year_int::text)::date,
	fy_end = ('08-31-' || fy_ut::text)::date;

--update row number
update reference_tables.ref_month_year a  
set my_row_counter = sub.rn 
from  (
select row_number() over(order by month_year_id) as rn
      ,*
from reference_tables.ref_month_year 
) sub
where sub.month_year_id = a.month_year_id;

--analyze it
analyze reference_tables.ref_month_year;

--select * from reference_tables.ref_month_year order by month_year_id desc;

/********************************
 * change update_log
 *******************************/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Table has dates from Jan 2007 to December 2030',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'reference_tables' and table_name = 'ref_month_year';

--select * from data_warehouse.update_log order by schema_name, table_name;
