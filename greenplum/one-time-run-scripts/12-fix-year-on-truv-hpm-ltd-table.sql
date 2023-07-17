/**************************
 * Legacy Truven HPM loading script for LTD table uses
 * 
 * update truven.htm_lt set year = extract(year from DTLAST)
 * 
 * but DTLAST has nulls, so instead we'll use DTABS1 (date of first absence)
 */

update truven.hpm_ltd set year = extract(year from DTABS1);

vacuum analyze truven.hpm_ltd;

/************
 * Rowcount
 * 
select year, count(*)
from truven.hpm_ltd
group by year
order by year;
 */

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
	details = 'Year changed to extract(year from dtabs1) instead of dtlast',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'truven' and table_name = 'hpm_ltd';

--select * from data_warehouse.update_log;
















