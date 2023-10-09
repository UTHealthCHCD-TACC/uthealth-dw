/********************************
 * This script adds any enrolids not already in dim_uth and assigns a new uth_member_id
 * 
 * Date  	|	Author 	| Change
 * ********************************************************************************
 * 07/13/23 | Xiaorui	| Truven split into truc and trum (commercial and medicare)
 */

/***********************
 * 07/13/23: As part of the truven data source split into truc and trum, 
 * we need to first delete all the records with data_source = 'truv'
 * 
delete from data_warehouse.dim_uth_member_id
where data_source = 'truv';
 */

--timestamp
select 'Truven dim_uth_member_id refresh started at ' || current_timestamp as message;
select 'mdcr started at ' || current_timestamp as message;

insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'trum' as v_raw_data
           from truven.mdcrt 
	 left outer join data_warehouse.dim_uth_member_id b 
          on b.data_source = 'trum'
         and b.member_id_src = enrolid::text
	   where b.member_id_src is null
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;

select 'mdcr done, starting ccae at ' || current_timestamp as message;

insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct enrolid as v_member_id, 'truc' as v_raw_data
           from truven.ccaet 
	 left outer join data_warehouse.dim_uth_member_id b 
          on b.data_source = 'truc'
         and b.member_id_src = enrolid::text
	   where b.member_id_src is null 	
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;

/* QA
 * 
select * from data_warehouse.dim_uth_member_id where data_source = 'trum';
select * from data_warehouse.dim_uth_member_id where data_source = 'truc';
 */

select 'claims refreshed, vacuum analyze and updating update_log: ' || current_timestamp as message;

vacuum analyze data_warehouse.dim_uth_member_id;

/********************************
 * change update_log
 *******************************/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date, --last updated 7/12/23
	details = 'Updated for Truven 2022 Q3, split Truven into truc and trum',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse' and table_name = 'dim_uth_member_id';

--timestamp
select 'Truven dim_uth_member_id refresh completed at ' || current_timestamp as message;



