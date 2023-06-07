/*
 * Creating enrollment table which includes general enrollment information, yearly condition flags, CRG scores, and COVID severity scores
 * Unlike with the master_claims table, we can create this table without splitting up into multiple steps (by year, data_source)
 */

delete from dev.ip_master_enrollment;

with enrl as(
select data_source, year, uth_member_id, gender_cd, race_cd, age_derived, 
		state, plan_type, bus_cd, total_enrolled_months
from data_warehouse.member_enrollment_yearly a 
where a.year >= 2014
),
cond  as (
select data_source, "year" , uth_member_id,
      aimm, ami, ca, cfib, chf, ckd, cliv, copd, 
		cysf, dep, epi, fbm, hemo, hep, hiv, 
		ihd, lbp, lymp, ms, nicu, pain, park, 
		pneu, ra, scd, smi, str, tbi, trans, 
		trau, asth, dem, diab, htn, opi, tob
from data_warehouse.conditions_member_enrollment_yearly 
where year >= 2014
),
crg as (
select
	cr.data_source,
	cr.uth_member_id,
	cr.crg_year,
	cr.crg,
	concat(left(crg, 1), right(crg, 1)) as crg_abbreviated
from data_warehouse.crg_risk cr
where cr.crg_year >= 2014
),
covid as (
select *
  from data_warehouse.covid_severity
)
insert into dev.ip_master_enrollment
select e.*, c.aimm, c.ami, c.ca, c.cfib, c.chf, c.ckd, c.cliv, c.copd, 
		c.cysf, c.dep, c.epi, c.fbm, c.hemo, c.hep, c.hiv, 
		c.ihd, c.lbp, c.lymp, c.ms, c.nicu, c.pain, c.park, 
		c.pneu, c.ra, c.scd, c.smi, c.str, c.tbi, c.trans, 
		c.trau, c.asth, c.dem, c.diab, c.htn, c.opi, c.tob, 
		cr.crg, cr.crg_abbreviated,
		cs.severity as covid_severity
 from enrl e
 join cond c
   on e.uth_member_id = c.uth_member_id
  and e.year = c.year
  and e.data_source = c.data_source
 left join crg cr
   on e.uth_member_id = cr.uth_member_id
  and e.year = cr.crg_year
--  and e.data_source = cr.data_source
 left join covid cs
   on e.uth_member_id = cs.uth_member_id
  and e.year = cs.year
  and e.data_source = cs.data_source
;

vacuum analyze dev.ip_master_enrollment;

----- Counts for QA
select *
from (
select 'master_enrollment' as table, data_source, year, count(distinct uth_member_id), count(*)
  from dev.ip_master_enrollment
 group by 2,3
union
select 'enrollment_only' as table, data_source, year, count(distinct uth_member_id), count(*)
  from data_warehouse.member_enrollment_yearly
 where year >= 2014
 group by 2,3
) a
order by 3,2,1;

select *
from (
select 'master_enrollment' as table, data_source, year, count(distinct uth_member_id) , count(*)
  from dev.ip_master_enrollment 
 where covid_severity is not null 
 group by 2,3
union
select 'severity' as table, data_source, year, count(distinct uth_member_id), count(*) 
  from tableau.dw_severity_2020 
 group by 2,3
) a
order by 3,2,1;

select *
from (
select 'master_enrollment' as table, data_source, year, count(distinct uth_member_id), count(*) 
 from dev.ip_master_enrollment 
group by 2,3
union
select 'conditions' as table, data_source, year, count(distinct uth_member_id), count(*) 
 from data_warehouse.conditions_member_enrollment_yearly
where year >= 2014
group by 2,3
) a
order by 3,2,1;

select *
from (
select 'master_enrollment' as table, data_source, year, count(distinct uth_member_id), count(*) 
 from dev.ip_master_enrollment
where crg is not null
group by 2,3
union
select 'crg' as table, data_source, crg_year, count(distinct uth_member_id), count(*) 
 from data_warehouse.crg_risk 
where crg_year >= 2014
group by 2,3
) a
order by 3,2,1;

-- change permissions instead
alter table tableau.master_enrollment owner to uthealth_dev;

grant select on tableau.master_enrollment to uthealth_analyst;

vacuum analyze tableau.master_enrollment;

select * from tableau.master_enrollment;
