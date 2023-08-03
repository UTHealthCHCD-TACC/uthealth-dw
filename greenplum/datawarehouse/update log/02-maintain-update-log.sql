/***************************************************************
 * This script updates the update log for data_warehouse
 *  - adds tables if they do not exist
 *  - refreshes vacuum analyze date
 * 
 * Author  || Date       || Note
 * *************************************************************
 * Xiaorui || 04/07/2023 || Created
 * *************************************************************
****************************************************************/

--Adds any new tables if they do not exist
insert into data_warehouse.update_log (schema_name, table_name, last_vacuum_analyze)
select schemaname, relname, 
	case when last_vacuum is not null then last_vacuum else last_analyze end as last_vacuum_analyze
from pg_stat_all_tables
where schemaname in ('data_warehouse', 'medicaid', 'medicare_national',
	'medicare_texas', 'optum_dod', 'optum_zip', 'reference_tables', 'truven')
and schemaname || relname not in (select schema_name || table_name from data_warehouse.update_log);
	
--update vacuum analyze
update data_warehouse.update_log a
set last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum
	else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname;
 
--vacuum analyze because why not
 vacuum analyze data_warehouse.update_log;

--look at it
select * from data_warehouse.update_log

/*******************
 * Remove dead tables that no longer exist
 */

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--delete dead tables
delete from data_warehouse.update_log
where not exists (
	select 1 from pg_catalog.pg_stat_all_tables
	where schemaname in ('data_warehouse', 'medicaid', 'medicare_national',
	'medicare_texas', 'optum_dod', 'optum_zip', 'reference_tables', 'truven')
	and schemaname || relname = update_log.schema_name || update_log.table_name
);

/****
 * Check what tables got deleted

select a.* from backup.update_log a left join data_warehouse.update_log b
on a.table_name = b.table_name and a.schema_name = b.schema_name
where b.table_name is null;
 */








