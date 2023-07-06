-- optum dod

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)          
with cte_distinct_claim as 
(
  select  distinct 'optd' as v_data_source, a.clmid::text as v_claim_id_src, a.member_id_src as v_member_id_src, 
          b.uth_member_id as v_uth_member_id, a."year" as v_data_year
  from optum_dod.medical a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optd'
    and b.member_id_src = a.member_id_src 
) 
select v_data_source, v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year
from cte_distinct_claim cdc
left outer join data_warehouse.dim_uth_claim_id c
  on c.data_source = v_data_source
 and c.claim_id_src = v_claim_id_src
 and c.member_id_src = v_member_id_src
 and c.data_year = v_data_year 
where c.uth_claim_id is null
;

vacuum analyze data_warehouse.dim_uth_claim_id;

-- optum zip

insert into data_warehouse.dim_uth_claim_id (data_source, claim_id_src, member_id_src, uth_member_id, data_year)          
with cte_distinct_claim as 
(
  select  distinct 'optz' as v_data_source, a.clmid::text as v_claim_id_src, a.member_id_src as v_member_id_src, 
          b.uth_member_id as v_uth_member_id, a."year" as v_data_year
  from optum_zip.medical a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'optz'
    and b.member_id_src = a.member_id_src 
) 
select v_data_source, v_claim_id_src, v_member_id_src, v_uth_member_id, v_data_year
from cte_distinct_claim cdc
left outer join data_warehouse.dim_uth_claim_id c
  on c.data_source = v_data_source
 and c.claim_id_src = v_claim_id_src
 and c.member_id_src = v_member_id_src
 and c.data_year = v_data_year 
where c.uth_claim_id is null
;

vacuum analyze data_warehouse.dim_uth_claim_id;