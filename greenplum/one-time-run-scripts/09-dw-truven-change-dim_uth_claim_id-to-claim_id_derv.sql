/**************************************
 * This script destroys and recreates dim_uth_claim_id to use claim_id_derv from Truven
 * 
 * Step 1: copy dim_uth_claim_id to dw_staging for all data sources other than truven
 * Step 2: Assign a uth_claim_id for each distinct claim_id_derv
 * Step 3: Do some schema swapping so that everything lands where it's supposed to
 **************************************/

--Make copy of table in dw_staging for data sources other than truven
drop table if exists dw_staging.dim_uth_claim_id;

create table dw_staging.dim_uth_claim_id as
select * from data_warehouse.dim_uth_claim_id
where data_source != 'truv'
distributed by (uth_claim_id);
--about 6 minutes

--insert into dw_staging.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id_src, data_year)
insert into dw_staging.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year) 
with all_clms as 
(
		select claim_id_derv as v_claim_id_src, a.enrolid::text as v_member_id_src, min(trunc(a.year,0)) as v_data_year                                      
		from truven.ccaeo a
		where a.enrolid is not null
		group by 1, 2
   union all                                      
		select claim_id_derv, a.enrolid::text, min(trunc(a.year,0))
		from truven.ccaes a
		where a.enrolid is not null
		group by 1, 2
   union all                               
		select claim_id_derv, a.enrolid::text,  min(trunc(a.year,0))
		from truven.mdcro a
		where a.enrolid is not null
		group by 1, 2
	union all
		select claim_id_derv, a.enrolid::text, min(trunc(a.year,0))
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
  left outer join dw_staging.dim_uth_claim_id c
    on c.data_source = 'truv'
   and c.claim_id_src = v_claim_id_src
   and c.member_id_src = v_member_id_src
   and c.data_year = v_data_year 
 where c.uth_claim_id is null;

vacuum analyze dw_staging.dim_uth_claim_id;

delete from dw_staging.dim_uth_claim_id
where data_source = 'truv' and member_id_src is null;

vacuum analyze dw_staging.dim_uth_claim_id;

--Run date 04/17/23 XRZ after truven 2022 Q1 & Q2 were loaded
--Updated Rows	