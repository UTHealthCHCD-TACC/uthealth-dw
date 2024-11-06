/*************************************************************************************************************
 * Script Purpose | This script adds any IQVIA pat_ids not already in the dim_uth_member_id table and 
 * _______________| assigns a new uth_member_id to them.
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 11/14/23 || Sharrah   || Script created - UTH Member ID is generated for ALL IQVIA pat_ids.
 * ---------++-----------++------------------------------------------------------------------------------------
 * 03/18/24 || Sharrah   || Script updated to add pat_ids that only exist in the iqvia.claims table to the 
 *          ||           || dim_uth_member_id table.
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           ||
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA data_warehouse.dim_uth_member_id refresh started at: ' || current_timestamp as message;


--=== Generate uth_member_ids for pat_ids in the iqvia.enroll2 table: ===--

insert into data_warehouse.dim_uth_member_id(member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct pat_id as v_member_id, 'iqva' as v_raw_data
    from iqvia.enroll2 
    	left outer join data_warehouse.dim_uth_member_id b 
    		on b.data_source = 'iqva' 
           and b.member_id_src = pat_id
	where b.member_id_src is null
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member;



--=== Generate uth_member_ids for pat_ids that only exist in the iqvia.claims table: ===--

insert into data_warehouse.dim_uth_member_id(member_id_src, data_source, uth_member_id, claim_created_id)
with cte_distinct_member as (
	select distinct pat_id as v_member_id, 'iqva' as v_raw_data
    from dev.sa_iqvia_derv_claimno_new_all_yr -- iqvia.claims table with the generated derv_claimnos
    	left outer join data_warehouse.dim_uth_member_id b 
    		on b.data_source = 'iqva' 
           and b.member_id_src = pat_id
	where b.member_id_src is null
)
select v_member_id, v_raw_data, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq'), true as claim_created_id
from cte_distinct_member;



--=== Vacuum analyze data_warehouse.dim_uth_member_id and update data_warehouse.update_log table: ===--

-- Timestamp: 
select 'Vacuum analyze data_warehouse.dim_uth_member_id started at: ' || current_timestamp as message;

-- Vacuum analyze:
vacuum analyze data_warehouse.dim_uth_member_id;

-- Timestamp:
select 'Update to data_warehouse.update_log started at: ' || current_timestamp as message;

-- Drop existing backup.update_log table:
drop table if exists backup.update_log;

-- Create backup of data_warehouse.update_log:
create table backup.update_log as
select * from data_warehouse.update_log;

-- Update update_log:
update data_warehouse.update_log a
set data_last_updated = current_date, -- last updated x/xx/xx
	details = 'Updated for IQVIA pat_ids 2006-2023',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse' and table_name = 'dim_uth_member_id';


-- Final timestamp:
select 'IQVIA data_warehouse.dim_uth_member_id refresh completed at ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various checks: =--

-- View Table:
--select * from data_warehouse.dim_uth_member_id where data_source = 'iqva' and claim_created_id is not true;
--select * from data_warehouse.dim_uth_member_id where data_source = 'iqva' and claim_created_id is true;

--``````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Total row count:
select 'data_warehouse.dim_uth_member_id total row count: ' as message, count(*) from data_warehouse.dim_uth_member_id where data_source = 'iqva'; -- CNT: 115690695

-- Total patient count (should be 115690695):
select 'data_warehouse.dim_uth_member_id total patient count (member_id_src): ' as message, count(distinct member_id_src) from data_warehouse.dim_uth_member_id where data_source = 'iqva'; -- CNT: 115690695
select 'data_warehouse.dim_uth_member_id total patient count (uth_member_id): ' as message, count(distinct uth_member_id) from data_warehouse.dim_uth_member_id where data_source = 'iqva'; -- CNT: 115690695

-- Total patient count from table union of enroll2 and claims (Should be 115690695):
select 'enroll2 union claims total patient count (pat_id)' as message, count(distinct pat_id)
from(
	select pat_id from iqvia.enroll2

	union 

	select pat_id from iqvia.claims
)a; -- CNT: 115690695

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Total patient count from enroll2:
select 'data_warehouse.dim_uth_member_id total enroll2 row count: ' as message, count(*) from data_warehouse.dim_uth_member_id where data_source = 'iqva' and claim_created_id is not true; -- CNT: 115613220
select 'data_warehouse.dim_uth_member_id enroll2 total patient count (member_id_src): ' as message, count(distinct member_id_src) from data_warehouse.dim_uth_member_id where data_source = 'iqva' and claim_created_id is not true; -- CNT: 115613220
select 'data_warehouse.dim_uth_member_id enroll2 total patient count (uth_member_id): ' as message, count(distinct uth_member_id) from data_warehouse.dim_uth_member_id where data_source = 'iqva' and claim_created_id is not true; -- CNT: 115613220

-- Enroll2 total patient count (should be 115613220):
select 'total patient count from iqvia.enroll2 (pat_id)' as message, count(distinct pat_id) from iqvia.enroll2; -- CNT: 115613220


--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Total patient count from claims:
select 'data_warehouse.dim_uth_member_id total claims row count: ' as message, count(*) from data_warehouse.dim_uth_member_id where data_source = 'iqva' and claim_created_id is true; -- CNT: 77475
select 'data_warehouse.dim_uth_member_id claims total patient count (member_id_src): ' as message, count(distinct member_id_src) from data_warehouse.dim_uth_member_id where data_source = 'iqva' and claim_created_id is true; -- CNT: 77475
select 'data_warehouse.dim_uth_member_id claims total patient count (uth_member_id): ' as message, count(distinct uth_member_id) from data_warehouse.dim_uth_member_id where data_source = 'iqva' and claim_created_id is true; -- CNT: 77475

-- Total patient count added from iqvia claims (should be 77475):
select 'total patient count from iqvia.claims (pat_id)' as message, count(distinct pat_id) 
from(
 select a.pat_id, b.pat_id as pat_id_b
 	from iqvia.claims a
	left join (select distinct pat_id from iqvia.enroll2) b
		on a.pat_id = b.pat_id
where b.pat_id is null)a; -- CNT: 77475

