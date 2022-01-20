drop table if exists tableau.crg_risk;
create table tableau.crg_risk as 
select
	cr.data_source,
	cr.uth_member_id,
	cr.crg_year,
	cr.crg,
	concat(left(crg, 1), right(crg, 1)) as crg_abbreviated,
	mey.gender_cd,
	mey.age_derived,
	mey.plan_type,
	mey.bus_cd,
	mey.state,
	mey.race_cd,
	mey.total_enrolled_months 
from
	data_warehouse.crg_risk cr
inner join data_warehouse.member_enrollment_yearly mey on
	mey.uth_member_id = cr.uth_member_id
	and mey."year" = cr.crg_year ;

