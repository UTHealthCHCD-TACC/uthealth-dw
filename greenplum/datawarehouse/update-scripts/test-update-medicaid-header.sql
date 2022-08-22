/*
* Medicaid claim header 
* claim type variable
* encounters and clms have different raw variables 
* logic from scripts 
* update maps
* easier to make temp tables 
* 1) make a copy of that part of the DW table in DEV 
-- check the mapping docs point 
2) test update copy
3) first just take limit 50000;
4) make a full dev table (just those data sources)
5) QA however makes sense 
6) whenever ur done let joe know look over it 
7) run it 
*/

drop table if exists dev.ip_medicaid_header_update;

-- 42s
select * 
into dev.ip_medicaid_header_update
from data_warehouse.claim_header 
where data_source = 'mdcd'
--limit 5000000;

-- medicaid claims 2m 42s
with cte_pos as (select max(pos) as pos, icn
				   from medicaid.clm_detail 
				   group by icn)
update dev.ip_medicaid_header_update c
   set claim_type = case when pos.pos = '1' then 'P' else 'F' end
  from medicaid.clm_header h
  join medicaid.clm_proc p
    on h.icn = p.icn
  join data_warehouse.dim_uth_claim_id u
    on u.claim_id_src = p.icn
   and u.member_id_src = p.pcn
   and u.data_source = 'mdcd'
  left outer join cte_pos as pos
    on h.icn = pos.icn
 where c.uth_claim_id = u.uth_claim_id;
 
-- medicaid encounters 2m 56s
update dev.ip_medicaid_header_update c
   set claim_type = case when h.tx_cd = 'P' then 'P'
   						when h.tx_cd = 'I' then 'F'
   						else 'n' end -- change to null when updating dw table
  from medicaid.enc_header h
  join medicaid.enc_proc p
    on h.derv_enc = p.derv_enc
  join data_warehouse.dim_uth_claim_id u
    on u.claim_id_src = p.derv_enc
   and u.member_id_src = p.mem_id
   and u.data_source = 'mdcd'
 where c.uth_claim_id = u.uth_claim_id;
 
-- QA, claim type counts for updated table and raw data 
-- 8s
with cte as (
select case when pos = '1' then 'P' else 'F' end as claim_type
 from (select max(pos) as pos, icn
	    from medicaid.clm_detail 
 	   group by icn) a
	   union all
	  select case when tx_cd = 'P' then 'P'
   					when tx_cd = 'I' then 'F'
   					else 'n' end
		from medicaid.enc_header)
select *
from (
select 'update', 'total' as claim_type, count(*)
  from dev.ip_medicaid_header_update
 group by 1,2
union
select 'update', claim_type, count(claim_type)
  from dev.ip_medicaid_header_update
 group by 1,2
union
select 'raw', 'total', count(*)
  from cte
group by 1,2
union
select 'raw', claim_type, count(claim_type)
  from cte
  group by 1,2) counts
order by 1,2;	