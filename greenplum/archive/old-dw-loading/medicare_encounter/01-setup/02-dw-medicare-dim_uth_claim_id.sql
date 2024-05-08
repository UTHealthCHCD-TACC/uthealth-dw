/**************************************
 * This script assigns new uth_claim_ids to claims that are not already in the table
 * from Medicare Encounter Texas and Medicare Encounter National
 * 
 * 05/23/23: Xiaorui added timestamps and auto-updating the update_log (with backup!)
 * Original code is from Will though
 * 
 * 02/06/24: Isrrael modified existing Medicare script for Medicare Encounter data
 **************************************/

select 'Medicare Encounter Texas dim_uth_claim_id script started at ' || current_timestamp as message;

---- ***** Medicare Encounter Texas***** 
---- These scripts check bcarrier, dme, hha, inpatient, outpatient, and snf tables
---wc002
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)
select 'mcet', enc_join_key, bene_id, raw_clms.uth_member_id, raw_clms.data_year 
from 
( 
    select enc_join_key, bene_id, uth_member_id, year::int2 as data_year
    from medicare_texas.carrier_base_enc a 
      join data_warehouse.dim_uth_member_id b 
        on b.data_source = 'mcet'
	   and b.member_id_src = bene_id
union 
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.dme_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcet'
	   and b.member_id_src = bene_id
union 	   
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.hha_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcet'
	   and b.member_id_src = bene_id
union 
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.ip_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcet'
	   and b.member_id_src = bene_id
union 
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.op_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcet'
	   and b.member_id_src = bene_id
union 
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.snf_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcet'
	   and b.member_id_src = bene_id
 ) raw_clms
   left outer join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = raw_clms.bene_id 
   and c.data_source = 'mcet'
   and c.claim_id_src = raw_clms.enc_join_key
where c.uth_claim_id is null 
;

select 'Medicare Encounter Texas dim_uth_claim_id script completed, Medicare Encounter National started at ' || current_timestamp as message;

---- ***** Medicare Encounter National***** 
---- These scripts check bcarrier, dme, hha, inpatient, outpatient, and snf tables
---wc002
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)
select 'mcen', enc_join_key, bene_id, raw_clms.uth_member_id, raw_clms.data_year 
from 
( 
    select enc_join_key, bene_id, uth_member_id, year::int2 as data_year
    from medicare_national.carrier_base_enc a 
      join data_warehouse.dim_uth_member_id b 
        on b.data_source = 'mcen'
	   and b.member_id_src = bene_id
union 
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.dme_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcen'
	   and b.member_id_src = bene_id
union 	   
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.hha_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcen'
	   and b.member_id_src = bene_id
union 
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.ip_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcen'
	   and b.member_id_src = bene_id
union 
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.op_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcen'
	   and b.member_id_src = bene_id
union 
	select enc_join_key, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.snf_base_enc a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcen'
	   and b.member_id_src = bene_id
 ) raw_clms
   left outer join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = raw_clms.bene_id 
   and c.data_source = 'mcen'
   and c.claim_id_src = raw_clms.enc_join_key
where c.uth_claim_id is null 
;

select 'Medicare Encounter National dim_uth_claim_id script completed at ' || current_timestamp as message;

vacuum analyze data_warehouse.dim_uth_claim_id;

--check last vacuum analyze to see if it worked
/*
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'data_warehouse' and relname = 'dim_uth_claim_id';
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
and schema_name = 'data_warehouse' and table_name = 'dim_uth_claim_id';
