/***********************************
* Script purpose: vacuum analyze all Truven tables
* except for reference tables and table_counts
* 
* 7/11/23: added code for updating tables
***********************************/

--timestamp
select 'Truven vacuum analyze script started at ' || current_timestamp as message;

--code to generate code to vacuum analyze all tables
select 'vacuum analyze ' || schemaname || '.' || relname || ';'
from pg_stat_all_tables
  where schemaname = 'truven' and
  (relname like 'ccae%' or relname like 'mdcr%' or relname like 'hpm%')
  order by n_live_tup;
  
--vacuum analyze all tables in ascending order of size
--smaller tables run just fine, ccaeo is the one that takes > 45 mins and hangs
vacuum analyze truven.hpm_ltd;
vacuum analyze truven.hpm_wc;
--vacuum analyze truven.mdcrp; These I don't think we're getting anymore... not in data quality reports
vacuum analyze truven.hpm_std;
vacuum analyze truven.mdcri;
--vacuum analyze truven.ccaep; same as above
vacuum analyze truven.ccaei;
vacuum analyze truven.mdcra;
vacuum analyze truven.hpm_elig;
vacuum analyze truven.hpm_abs;
vacuum analyze truven.mdcrs;
vacuum analyze truven.mdcrt;
vacuum analyze truven.ccaea;
vacuum analyze truven.mdcrf;
vacuum analyze truven.ccaes;
vacuum analyze truven.mdcrd;
vacuum analyze truven.mdcro;
vacuum analyze truven.ccaef;
vacuum analyze truven.ccaed;
vacuum analyze truven.ccaet;
vacuum analyze truven.ccaeo;

/********************************
 * change update_log
 *******************************/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date, --last updated 7/11/23
	details = 'Raw data updated for 2022 Q1 - Q3',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'truven' and 
	(table_name like 'ccae%' or table_name like 'mdcr%' or table_name like 'hpm%') and
	table_name not like '%p';

select * from data_warehouse.update_log;

--timestamp
select 'Vacuum analyze completed at ' || current_timestamp as message;





