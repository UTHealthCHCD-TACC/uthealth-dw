-- optum dod
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct a.member_id_src as v_member_id, 'optd' as v_data_source
	from optum_dod.mbr_enroll_r a 
	left outer join data_warehouse.dim_uth_member_id b 
	  on b.data_source = 'optd'
	 and b.member_id_src = a.member_id_src 
	where b.uth_member_id is null 
)
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;

--Run date 06/22/2023 by IP
-- Updated Rows	2467677

-- optum zip
insert into data_warehouse.dim_uth_member_id (member_id_src, data_source, uth_member_id)
with cte_distinct_member as (
	select distinct a.member_id_src as v_member_id, 'optz' as v_data_source
	from optum_zip.mbr_enroll a 
	left outer join data_warehouse.dim_uth_member_id b 
	  on b.data_source = 'optz'
	 and b.member_id_src = a.member_id_src 
	where b.uth_member_id is null 
)
select v_member_id, v_data_source, nextval('data_warehouse.dim_uth_member_id_uth_member_id_seq')
from cte_distinct_member 
;

--Run date 06/22/2023 by IP
--Updated Rows	2467677
