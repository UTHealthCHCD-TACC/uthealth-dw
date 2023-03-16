drop table if exists tableau.master_enrollment;

create table tableau.master_enrollment
with (appendoptimized=true, orientation=column, compresstype=zlib)
as
with enrl as(
select data_source, year, uth_member_id, gender_cd, race_cd, age_derived, state, plan_type, bus_cd, total_enrolled_months
from data_warehouse.member_enrollment_yearly a 
where a.year between 2014 and 2021
  and a.data_source in ('optz', 'truv','mcrt','mcrn', 'mdcd')
),
cond  as (
select a.data_source, a."year" , a.uth_member_id,
      a.aimm, a.ami,  a.ca, a.cfib, a.chf, a.ckd, a.cliv, a.copd, a.cysf, 
       a.dep, a.epi, a.fbm, a.hemo, a.hep,  a.hiv, a.ihd,  a.lbp, 
      a.lymp, a.ms, a.nicu, a.pain, a.park, a.pneu,a.ra, a.scd, a.smi, a.str, a.tbi, a.trans, a.trau
from data_warehouse.conditions_member_enrollment_yearly a 
where a.year between 2014 and 2021
  and a.data_source in ('optz', 'truv','mcrt','mcrn', 'mdcd')
),
crg as (
select
	cr.data_source,
	cr.uth_member_id,
	cr.crg_year,
	cr.crg,
	concat(left(crg, 1), right(crg, 1)) as crg_abbreviated
from data_warehouse.crg_risk cr
inner join data_warehouse.member_enrollment_yearly mey 
  on  mey.uth_member_id = cr.uth_member_id
	and mey."year" = cr.crg_year 
where mey.year between 2014 and 2021
  and mey.data_source in ('optz', 'truv','mcrt','mcrn', 'mdcd')
),
covid as (
select *
  from tableau.dw_severity_2020
 where data_source in ('optz', 'truv','mcrt','mcrn', 'mdcd')
)
select e.*, c.aimm, c.ami,  c.ca, c.cfib, c.chf, c.ckd, c.cliv, c.copd, c.cysf, 
       c.dep, c.epi, c.fbm, c.hemo, c.hep, c.hiv, c.ihd, c.lbp, 
      c.lymp, c.ms, c.nicu, c.pain, c.park, c.pneu,c.ra, c.scd, c.smi, c.str, c.tbi, c.trans, c.trau,
      cr.crg, cr.crg_abbreviated,
      cs.severity as covid_severity
 from enrl e
 left join cond c
   on e.uth_member_id = c.uth_member_id
  and e.year = c.year
 left join crg cr
   on e.uth_member_id = cr.uth_member_id
  and e.year = cr.crg_year
 left join covid cs
   on e.uth_member_id = cs.uth_member_id
  and e.year = cs.year
distributed by (uth_member_id);



----- Counts for QA
select *
from (
select 'master_enrollment' as table, data_source, year, count(distinct uth_member_id)
  from tableau.master_enrollment 
 group by 2,3
union
select 'enrollment_only' as table, data_source, year, count(distinct uth_member_id) 
  from data_warehouse.member_enrollment_yearly
 where data_source in ('optz', 'truv','mcrt','mcrn', 'mdcd')
   and year between 2014 and 2021
 group by 2,3
) a
order by 3,2,1;

select *
from (
select 'master_enrollment' as table, data_source, year, count(distinct uth_member_id) 
  from tableau.master_enrollment 
 where cov_severity is not null 
 group by 2,3
union
select 'severity' as table, data_source, year, count(distinct uth_member_id) 
  from tableau.dw_severity_2020 
 where data_source in ('optz', 'truv','mcrt','mcrn')
 group by 2,3
) a
order by 3,2,1;

select *
from (
select 'master_enrollment' as table, data_source, year, count(distinct uth_member_id) 
 from tableau.master_enrollment 
group by 2,3
union
select 'conditions' as table, data_source, year, count(distinct uth_member_id) 
 from tableau.member_conditions 
where data_source in ('optz', 'truv','mcrt','mcrn')
  and year between 2012 and 2021 
group by 2,3
) a
order by 3,2,1;

select *
from (
select 'master_enrollment' as table, data_source, year, count(distinct uth_member_id) 
 from tableau.master_enrollment
where crg is not null
group by 2,3
union
select 'crg' as table, data_source, crg_year, count(distinct uth_member_id) 
 from data_warehouse.crg_risk 
where data_source in ('optz', 'truv','mcrt','mcrn')
  and crg_year between 2012 and 2021 
group by 2,3
) a
order by 3,2,1;

-- change permissions instead
alter table tableau.master_enrollment owner to uthealth_analyst;

analyze tableau.master_enrollment;

select * from tableau.master_enrollment;

with official as (
select table_name, year, frequency
from qa_reporting.truven_official_counts
where "column" = 'year')
select a.year, a.table_name, a.row_count, frequency, frequency - a.row_count
from qa_reporting.truven_counts a
full join official b
  on a.year = b.year
  and a.table_name = b.table_name
  order by 2,1;
