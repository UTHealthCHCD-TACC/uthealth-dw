-- add raw member id and claim id to acute admission tables

alter table data_warehouse.admission_acute_ip add column member_id_src text;

update data_warehouse.admission_acute_ip a
set member_id_src = b.member_id_src
from data_warehouse.dim_uth_member_id b
where case when a.data_source in ('mhtw', 'mcpp') then 'mdcd' else a.data_source end = b.data_source
and a.uth_member_id = b.uth_member_id;


alter table data_warehouse.admission_acute_ip_claims add column member_id_src text;
alter table data_warehouse.admission_acute_ip_claims add column claim_id_src text;

update data_warehouse.admission_acute_ip_claims a
set member_id_src = b.member_id_src,
	claim_id_src = b.claim_id_src
from data_warehouse.dim_uth_claim_id b
where case when a.data_source in ('mhtw', 'mcpp') then 'mdcd' else a.data_source end = b.data_source
and a.uth_member_id = b.uth_member_id
and a.uth_claim_id = b.uth_claim_id
;

-- update costs

with costs as (
	select b.uth_member_id, b.uth_claim_id, b.from_date_of_service, 
			sum(a.charge_amount) total_charge_amount, sum(a.allowed_amount) total_allowed_amount, sum(a.paid_amount) total_paid_amount
	from data_warehouse.claim_detail a
	join data_warehouse.admission_acute_ip_claims b
	on a.uth_member_id = b.uth_member_id
	and a.uth_claim_id = b.uth_claim_id
	group by 1,2,3
)
update data_warehouse.admission_acute_ip_claims a
set charge_amount = b.total_charge_amount,
	allowed_amount = b.total_allowed_amount,
	paid_amount = b.total_paid_amount
from costs b
where a.uth_member_id = b.uth_member_id
and a.uth_claim_id = b.uth_claim_id
and a.from_date_of_service = b.from_date_of_service
;

with costs as (
	select derived_uth_admission_id, sum(charge_amount) total_charge_amount, sum(allowed_amount) total_allowed_amount, sum(paid_amount) total_paid_amount
	from data_warehouse.admission_acute_ip_claims
	group by derived_uth_admission_id
)
update data_warehouse.admission_acute_ip a
set total_charge_amount = b.total_charge_amount,
	total_allowed_amount = b.total_allowed_amount,
	total_paid_amount = b.total_paid_amount
from costs b
where a.derived_uth_admission_id = b.derived_uth_admission_id;


vacuum analyze data_warehouse.admission_acute_ip;
vacuum analyze data_warehouse.admission_acute_ip_claims;