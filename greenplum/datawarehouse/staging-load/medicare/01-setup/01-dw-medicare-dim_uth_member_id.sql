/**************************************
 * This script assigns new uth_member_ids to members that are not already in the table
 * from Medicare Texas and Medicare National
 * 
 * 05/23/23: Xiaorui added timestamps and auto-updating the update_log (with backup!)
 * Original code is from Will/Joe W
 * 
 * This script is lightning-fast, btw
 * 
 **************************************/

select 'Medicare Texas dim_uth_member_id script started at ' || current_timestamp as message;

-- ***** Medicare Texas ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mcrt' as v_raw_data
	from medicare_texas.mbsf_abcd_summary
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source = 'mcrt'
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

select 'Medicare Texas dim_uth_member_id script finished, Medicare National started at ' || current_timestamp as message;

--- ***** Medicare National ***** 
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct bene_id as v_member_id, 'mcrn' as v_raw_data
	from medicare_national.mbsf_abcd_summary
	 left outer join data_warehouse.dim_uth_member_id b 
      on b.data_source in ('mcrn','mcrt')
     and b.member_id_src = bene_id::text
    where b.member_id_src is null 
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member
;

select 'Medicare National dim_uth_member_id script finished at ' || current_timestamp as message;

vacuum analyze data_warehouse.dim_uth_member_id;

--check last vacuum analyze to see if it worked
/*
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'data_warehouse' and relname = 'dim_uth_member_id';
*/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Medicare Texas and Medicare National updated',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse' and table_name = 'dim_uth_member_id';

vacuum analyze data_warehouse.update_log;

