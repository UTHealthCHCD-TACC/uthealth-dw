/*************************************************************************************************************
 * Script Purpose | This script adds any IQVIA derv_claimnos not already in the dim_uth_claim_id table and 
 * _______________| assigns a new uth_claim_id to them.
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 1/09/23  || Sharrah   || Script created - UTH claim ID is generated for ALL IQVIA derv_claimnos.
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           ||
 **************************************************************************************************************/ 

--=== Generate UTH claim ID for derv_claimnos in the iqvia.claims table ===--

-- Timestamp:
select 'IQVIA data_warehouse.dim_uth_claim_id refresh started at: ' || current_timestamp as message;

insert into data_warehouse.dim_uth_claim_id(data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
with all_clms as 
(                            
	select derv_claimno as v_claim_id_src, pat_id as v_member_id_src, min(substring(from_dt, 1, 4)::int) as v_data_year
	from dev.sa_iqvia_derv_claimno -- iqvia.claims table with the generated derv_claimnos
	where (new_rectype != 'P' or new_rectype is null) -- filter for medical claims only
		and pat_id is not null
		and derv_claimno is not null
	group by 1, 2
),
cte_distinct_iqvia_claim as 
(
	select distinct a.v_claim_id_src, a.v_member_id_src, b.uth_member_id as v_uth_member_id, v_data_year
  	from all_clms a 
   		join data_warehouse.dim_uth_member_id b 
    		on b.data_source = 'iqva'
   		   and a.v_member_id_src = b.member_id_src
)
select 'iqva', v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year 
from cte_distinct_iqvia_claim 
 	left outer join data_warehouse.dim_uth_claim_id c
    	on c.data_source = 'iqva'
   	   and c.claim_id_src = v_claim_id_src
   	   and c.member_id_src = v_member_id_src
   	   and c.data_year = v_data_year 
where c.uth_claim_id is null;



--=== Vacuum analyze data_warehouse.dim_uth_claim_id and update data_warehouse.update_log table: ===--

-- Timestamp:
select 'Vacuum analyze data_warehouse.dim_uth_claim_id started at: ' || current_timestamp as message;

-- Vacuum Anlyze:
vacuum analyze data_warehouse.dim_uth_claim_id;

-- Timestamp:
select 'Update to data_warehouse.update_log started at: ' || current_timestamp as message;

-- Drop existing backup.update_log table:
drop table if exists backup.update_log;

-- Create backup of data_warehouse.update_log:
create table backup.update_log as
select * from data_warehouse.update_log;

-- Update update_log:
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Updated for IQVIA derv_claimnos 2006-2023',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse' and table_name = 'dim_uth_claim_id';


-- Final timestamp:
select 'IQVIA dim_uth_claim_id refresh completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

/*

-- View Table:
select * from data_warehouse.dim_uth_claim_id where data_source = 'iqva' order by 1;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Row count:
select 'total row count in data_warehouse.dim_uth_claim_id table' as message, count(*) from data_warehouse.dim_uth_claim_id where data_source = 'iqva'; -- CNT: # 4170185799

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Compare total claimno counts (should equal the row count, #):
select 'total claim count (uth_claim_id) in data_warehouse.dim_uth_claim_id table' as message, count(distinct uth_claim_id) from data_warehouse.dim_uth_claim_id where data_source = 'iqva'; -- CNT: # 4170185799
select 'total claim count (claim_id_src) in data_warehouse.dim_uth_claim_id table' as message, count(distinct claim_id_src) from data_warehouse.dim_uth_claim_id where data_source = 'iqva'; -- CNT: # 4170185799

-- total claim count from iqvia raw claims table (should be #):
select 'total claim count in raw iqvia claims table' as message, count(distinct derv_claimno)
from dev.sa_iqvia_derv_claimno
where (new_rectype != 'P' or new_rectype is null); -- CNT: # 4170185799

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Compare total patient counts:
select 'total pat count (uth_member_id) in data_warehouse.dim_uth_claim_id table' as message, count(distinct uth_member_id) from data_warehouse.dim_uth_claim_id where data_source = 'iqva'; -- CNT: # 94178429
select 'total pat count (member_id_src) in data_warehouse.dim_uth_claim_id table' as message, count(distinct member_id_src) from data_warehouse.dim_uth_claim_id where data_source = 'iqva'; -- CNT: # 94178429

-- total patient count from iqvia raw claims table (should be #):
select 'total pat count in raw iqvia claims table' as message, count(distinct pat_id) 
from dev.sa_iqvia_derv_claimno 
where (new_rectype != 'P' or new_rectype is null); -- CNT: # 94178429

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct Year:
select distinct data_year from data_warehouse.dim_uth_claim_id where data_source = 'iqva' order by 1; -- Years: 2006 thru 2023

-- Check for nulls:
select * from data_warehouse.dim_uth_claim_id where data_source = 'iqva' and uth_claim_id is null; -- no rows returned, no nulls
select * from data_warehouse.dim_uth_claim_id where data_source = 'iqva' and claim_id_src is null or claim_id_src  = ''; -- no rows returned, no nulls
select * from data_warehouse.dim_uth_claim_id where data_source = 'iqva' and uth_member_id  is null; -- no rows returned, no nulls
select * from data_warehouse.dim_uth_claim_id where data_source = 'iqva' and uth_member_id  is null or claim_id_src  = ''; -- no rows returned, no nulls

*/
