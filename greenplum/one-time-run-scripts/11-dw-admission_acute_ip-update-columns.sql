alter table data_warehouse.admission_acute_ip add column member_id_src text;

update data_warehouse.admission_acute_ip a
set member_id_src = b.member_id_src
from data_warehouse.dim_uth_member_id b
where a.data_source = b.data_source
and a.uth_member_id = b.uth_member_id;


alter table data_warehouse.admission_acute_ip_claims add column member_id_src text;
alter table data_warehouse.admission_acute_ip_claims add column claim_id_src text;


update data_warehouse.admission_acute_ip_claims a
set member_id_src = b.member_id_src,
	claim_id_src = b.claim_id_src
from data_warehouse.dim_uth_claim_id b
where a.data_source = b.data_source
and a.uth_member_id = b.uth_member_id
and a.uth_claim_id = b.uth_claim_id
and a."year" = b.data_year;