/**************************************
 * This script destroys and recreates dim_uth_claim_id to use claim_id_derv from Truven
 * 
 * Step 1: copy dim_uth_claim_id to dw_staging for all data sources other than truven
 * Step 2: Assign a uth_claim_id for each distinct claim_id_derv
 * Step 3: Do some schema swapping so that everything lands where it's supposed to
 **************************************/

--NOTE: This script took ~ 3 hrs when I ran it in the middle of the night
--Last run date 04/18/23 XRZ after truven 2022 Q1 & Q2 were loaded

--create backup
drop table if exists backup.dim_uth_claim_id_truven;

create table backup.dim_uth_claim_id_truven as
select * from data_warehouse.dim_uth_claim_id
where data_source = 'truv';

--deleteeeeee
delete from data_warehouse.dim_uth_claim_id
where data_source = 'truv';

--insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id_src, data_year)
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
with all_clms as 
(
		select a.claim_id_derv as v_claim_id_src, a.enrolid::text as v_member_id_src, min(trunc(a.year,0)) as v_data_year                                      
		from truven.ccaeo a
		where a.enrolid is not null
		group by 1, 2
   union all                                      
		select a.claim_id_derv, a.enrolid::text, min(trunc(a.year,0))
		from truven.ccaes a
		where a.enrolid is not null
		group by 1, 2
   union all                               
		select a.claim_id_derv, a.enrolid::text,  min(trunc(a.year,0))
		from truven.mdcro a
		where a.enrolid is not null
		group by 1, 2
	union all
		select a.claim_id_derv, a.enrolid::text, min(trunc(a.year,0))
		from truven.mdcrs a
		where a.enrolid is not null
		group by 1, 2
),
cte_distinct_truven_claim as 
(
select distinct a.v_claim_id_src, a.v_member_id_src, b.uth_member_id as v_uth_member_id, v_data_year
  from all_clms a 
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truv'
   and a.v_member_id_src = b.member_id_src
)
select 'truv', v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year 
from cte_distinct_truven_claim 
  left outer join data_warehouse.dim_uth_claim_id c
    on c.data_source = 'truv'
   and c.claim_id_src = v_claim_id_src
   and c.member_id_src = v_member_id_src
   and c.data_year = v_data_year 
 where c.uth_claim_id is null;

vacuum analyze data_warehouse.dim_uth_claim_id;

--check last vacuum analyze to see if it worked

select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'data_warehouse' and relname = 'dim_uth_claim_id';

--This finished around 1 AM after I left it running O/N

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Truven: claim_id_src maps to claim_id_derv, updated for 2022 Q1 & Q2',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse' and table_name = 'dim_uth_claim_id';


