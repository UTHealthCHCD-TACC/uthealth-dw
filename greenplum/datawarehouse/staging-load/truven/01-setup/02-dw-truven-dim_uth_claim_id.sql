/**************************************
 * This script assigns new uth_claim_ids to claims that are not already in the table from Truven
 * 
 * Changelog
 * 
 * Date		|	Author		|	Change
 * *********************************************************************
 * 04/18/23	| Xiaorui 		| Modified to use claim_id_derv instead of msclmid
 * 							  Also added auto-updating the update_log (with backup!)
 * ------------------------------------------------------------------------------------
 * 07/13/23	| Xiaorui		| Split 'truv' into 'trum' and 'truc'
 **************************************/

/***********************
 * 07/13/23: As part of the truven data source split into truc and trum, 
 * we need to first delete all the records with data_source = 'truv'
 * 
delete from data_warehouse.dim_uth_claim_id where data_source = 'truv';
vacuum analyze data_warehouse.dim_uth_claim_id;
 */

/*
--how distinct are 'o' tables and 's' tables? are there claims that are in both?
select a.claim_id_derv as s_claim, b.claim_id_derv as o_claim 
from truven.mdcrs a full outer join truven.mdcro b on a.claim_id_derv = b.claim_id_derv
where a.claim_id_derv is not null and b.claim_id_derv is not null;
--not distinct, there are claims that exist in both tables
*/

--timestamp
select 'Truven dim_uth_claim_id refresh started at ' || current_timestamp as message;

--mdcr claims
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
with all_clms as 
(                            
		select claim_id_derv as v_claim_id_src, enrolid::text as v_member_id_src, min(trunc(year,0)) as v_data_year
		from truven.mdcro
		where enrolid is not null
		and claim_id_derv is not null
		group by 1, 2
	union all
		select claim_id_derv, enrolid::text, min(trunc(year,0))
		from truven.mdcrs
		where enrolid is not null
		and claim_id_derv is not null
		group by 1, 2
),
cte_distinct_truven_claim as 
(
select distinct a.v_claim_id_src, a.v_member_id_src, b.uth_member_id as v_uth_member_id, v_data_year
  from all_clms a 
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'trum'
   and a.v_member_id_src = b.member_id_src
)
select 'trum', v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year 
from cte_distinct_truven_claim 
  left outer join data_warehouse.dim_uth_claim_id c
    on c.data_source = 'trum'
   and c.claim_id_src = v_claim_id_src
   and c.member_id_src = v_member_id_src
   and c.data_year = v_data_year 
 where c.uth_claim_id is null;

vacuum analyze data_warehouse.dim_uth_claim_id;

--select * from data_warehouse.dim_uth_claim_id where data_source = 'trum';

select 'mdcr claims refreshed, ccae started at ' || current_timestamp as message;

--ccae claims
insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
with all_clms as 
(                            
		select claim_id_derv as v_claim_id_src, enrolid::text as v_member_id_src, min(trunc(year,0)) as v_data_year
		from truven.ccaeo
		where enrolid is not null
		and claim_id_derv is not null
		group by 1, 2
	union all
		select claim_id_derv, enrolid::text, min(trunc(year,0))
		from truven.ccaes
		where enrolid is not null
		and claim_id_derv is not null
		group by 1, 2
),
cte_distinct_truven_claim as 
(
select distinct a.v_claim_id_src, a.v_member_id_src, b.uth_member_id as v_uth_member_id, v_data_year
  from all_clms a 
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'truc'
   and a.v_member_id_src = b.member_id_src
)
select 'truc', v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year 
from cte_distinct_truven_claim 
  left outer join data_warehouse.dim_uth_claim_id c
    on c.data_source = 'truc'
   and c.claim_id_src = v_claim_id_src
   and c.member_id_src = v_member_id_src
   and c.data_year = v_data_year 
 where c.uth_claim_id is null;

select 'claims refreshed, vacuum analyze and updating update_log: ' || current_timestamp as message;

vacuum analyze data_warehouse.dim_uth_claim_id;

select * from data_warehouse.dim_uth_claim_id
where data_source = 'truc';

/*check last vacuum analyze to see if it worked

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
	details = 'Updated for Truven 2022 Q3, split Truven into truc and trum',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse' and table_name = 'dim_uth_claim_id';

--timestamp
select 'Truven dim_uth_claim_id refresh completed at ' || current_timestamp as message;






