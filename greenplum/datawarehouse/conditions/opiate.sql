
-- diagnosis codes for qualification
create table dev.gm_condition_opi as
with opi_dx as (select uth_member_id, year from conditions.person_profile_work_table where condition_cd = 'OPI')
select uth_member_id, year from opi_dx group by uth_member_id, year;

-- opiod medication dispensing events (claims with opiod) with a total of 90 days supply or greater
with opi_icd as (
select
	ndc
from
	conditions.condition_ndc cn
where
	condition_cd = 'OPI'),
opi_rx_claims as (
select
	data_source,
	uth_member_id,
	pc.days_supply,
	year
from
	data_warehouse.pharmacy_claims pc
inner join opi_icd on
	opi_icd.ndc = pc.ndc)
insert into dev.gm_condition_opi(uth_member_id, year) 
select
	uth_member_id,
	year
from
	opi_rx_claims
group by
data_source,
	uth_member_id,
	year
having count(*) >= 90;

-- exclude cancer patients
delete from dev.gm_condition_opi where uth_member_id in (select distinct uth_member_id from conditions.member_enrollment_yearly mey where ca = 1);

select * from dev.gm_condition_opi order by uth_member_id, year;

select * from data_warehouse.pharmacy_claims pc;

select * from truven.ccaed c  where member_id_src ='1456433402' and ndcnum = 378912398;