/**************************************
 * This script assigns new uth_claim_ids to claims that are not already in the table
 * from Medicare Texas and Medicare National
 * 
 * 05/23/23: Xiaorui added timestamps and auto-updating the update_log (with backup!)
 * Original code is from Will though
 * 
 **************************************/

select 'Medicare Texas dim_uth_claim_id script started at ' || current_timestamp as message;

---- ***** Medicare Texas***** 
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient, and snf tables
---wc002
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)
select 'mcrt', clm_id, bene_id, raw_clms.uth_member_id, raw_clms.data_year 
from 
( 
    select clm_id, bene_id, uth_member_id, year::int2 as data_year
    from medicare_texas.bcarrier_claims_k a 
      join data_warehouse.dim_uth_member_id b 
        on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.dme_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 	   
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.hha_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.hospice_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.inpatient_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.outpatient_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_texas.snf_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrt'
	   and b.member_id_src = bene_id
 ) raw_clms
   left outer join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = raw_clms.bene_id 
   and c.data_source = 'mcrt'
   and c.claim_id_src = raw_clms.clm_id
where c.uth_claim_id is null 
;

select 'Medicare Texas dim_uth_claim_id script completed, Medicare National started at ' || current_timestamp as message;

---- ***** Medicare National***** 
---- These scripts check bcarrier, dme, hha, hospice, inpatient, outpatient, and snf tables
---wc002
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)
select 'mcrn', clm_id, bene_id, raw_clms.uth_member_id, raw_clms.data_year 
from 
( 
    select clm_id, bene_id, uth_member_id, year::int2 as data_year
    from medicare_national.bcarrier_claims_k a 
      join data_warehouse.dim_uth_member_id b 
        on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.dme_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 	   
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.hha_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.hospice_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.inpatient_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.outpatient_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
union 
	select clm_id, bene_id, b.uth_member_id, a.year::int2
	from medicare_national.snf_base_claims_k a
	  join data_warehouse.dim_uth_member_id b 
	    on b.data_source = 'mcrn'
	   and b.member_id_src = bene_id
 ) raw_clms
   left outer join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = raw_clms.bene_id 
   and c.data_source = 'mcrn'
   and c.claim_id_src = raw_clms.clm_id
where c.uth_claim_id is null 
;

select 'Medicare National dim_uth_claim_id script completed at ' || current_timestamp as message;

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
