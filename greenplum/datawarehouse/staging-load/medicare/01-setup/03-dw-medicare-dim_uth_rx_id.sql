/**************************************
 * This script assigns new uth_rx_claim_ids to claims that are not already in the table
 * from Medicare Texas and Medicare National
 * 
 * 05/23/23: Xiaorui added timestamps and auto-updating the update_log (with backup!)
 * And fixed a typo for adding mcrt claims - it was pulling data from the medicare national pde_file
 * Original code is from Will though
 * 
 * ~ 5 minutes
 * 
 * --10/9/23 XZ: noted that script causes dupes to emerge, but haven't figured out why yet.
 * Check if it happens again on next load. Might have something to do with year - we had issues
 * with rx claims in 2019 whose member enrollment is in 2020 being duplicated
 * 
 **************************************/

select 'Medicare Texas dim_uth_rx_claim_id script started at ' || current_timestamp as message;

with cte as (  
    select distinct on (pde_id) 
        year, pde_id, bene_id 
    from medicare_texas.pde_file
    )
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 					
select 'mcrt'
       ,a.year::int
       ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	   ,a.pde_id
	   ,b.uth_member_id
	   ,a.bene_id 
from cte a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = a.bene_id
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'mcrt'
 and c.member_id_src = a.bene_id 
 and c.rx_claim_id_src = a.pde_id
where c.uth_rx_claim_id is null 
  and a.bene_id is not null
 ;

select 'Medicare Texas finished, Medicare National started at ' || current_timestamp as message;

---Medicare National
with cte as (  
    select distinct on (pde_id) 
        year, pde_id, bene_id 
    from medicare_national.pde_file
    )
insert into data_warehouse.dim_uth_rx_claim_id (
			 data_source
			,year 
			,uth_rx_claim_id
			,rx_claim_id_src
			,uth_member_id
			,member_id_src ) 					
select 'mcrn'
       ,a.year::int
       ,nextval('data_warehouse.dim_uth_rx_claim_id_uth_rx_claim_id_seq')
	   ,a.pde_id
	   ,b.uth_member_id
	   ,a.bene_id 
from cte a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrn'
   and b.member_id_src = a.bene_id
left join data_warehouse.dim_uth_rx_claim_id c 
  on c.data_source = 'mcrn'
 and c.member_id_src = a.bene_id 
 and c.rx_claim_id_src = a.pde_id
where c.uth_rx_claim_id is null 
  and a.bene_id is not null
 ;

select 'Medicare National dim_uth_rx_claim_id script finished at ' || current_timestamp as message;

vacuum analyze data_warehouse.dim_uth_rx_claim_id;

--check last vacuum analyze to see if it worked
/*
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'data_warehouse' and relname = 'dim_uth_rx_claim_id';
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
and schema_name = 'data_warehouse' and table_name = 'dim_uth_rx_claim_id';













