/***************************************************************
 * This script creates an update log for data_warehouse
 * 
 * Author  || Date       || Note
 * *************************************************************
 * Xiaorui || 03/16/2023 || Created
 * *************************************************************
 * Xiaorui || 03/23/2023 || Added Details column
****************************************************************/

--deletes table
drop table if exists data_warehouse.update_log;

--Creates update log
create table data_warehouse.update_log (
	schema_name text,
	table_name text,
	data_last_updated date,
	last_vacuum_analyze date,
	details text
)
distributed by (table_name);

--pulls data from pg_stat - we won't have last update date for data but we can at least
--get the last vacuum analyze date
insert into data_warehouse.update_log (schema_name, table_name, last_vacuum_analyze)
select schemaname, relname, 
	case when last_vacuum is not null then last_vacuum else last_analyze end as last_vacuum_analyze
from pg_stat_all_tables
where schemaname in ('data_warehouse', 'medicaid', 'medicare_national',
	'medicare_texas', 'optum_dod', 'optum_zip', 'reference_tables', 'truven');
	
--Niall/Lopita added truven_pay data 3/3/23 (includes new column: MSA)
--this is only for the base tables - p (population) tables were not updated
update data_warehouse.update_log
set data_last_updated = '4/10/23'::date,
	details = 'Truven 2022 Q1 and Q2 updated'
where schema_name = 'truven' and
 ((table_name like 'ccae%' and table_name != 'ccaep') or
  (table_name like 'mdcr%' and table_name != 'mdcrp'));
 
--see table
select * from data_warehouse.update_log
order by schema_name, table_name;

--vacuum analyze because why not
 vacuum analyze data_warehouse.update_log;



