--hypertension
-- Search in any facility or professional claim for ICD 9 DX  (primary or other) in 3 or more claims
-- Search in any facility or professional claim for ICD 10 DX  (primary or other) in 3 or more claims

drop table if exists conditions.xl_condition_htn_dx_1;
create table conditions.xl_condition_htn_dx_1 as
with htn_dx as (select
		diag_cd
	from
		conditions.diagnosis_codes_list dcl
	where
		dcl.condition_cd = 'htn')
select
	uth_member_id,
	uth_claim_id,
	min(diag_position) diag_position,
	min(extract(year from from_date_of_service)) year_of_service
from
	data_warehouse.claim_diag cd
inner join  htn_dx
on
	cd.diag_cd = htn_dx.diag_cd
group by
	uth_member_id,
	uth_claim_id;

create table conditions.xl_condition_htn_dx_output as 
with qual_years as (select
	uth_member_id,
	year_of_service,
	count(*)
from
	conditions.xl_condition_htn_dx_1
group by
	uth_member_id,
	year_of_service
having
	count(*) >= 3)
select uth_member_id, min(year_of_service) as initial_htn_year from qual_years
group by uth_member_id;
