
------------------
drop table if exists tableau.dashboard_1720;


create table tableau.dashboard_1720
with (appendoptimized=true, orientation=column, compresstype=zlib)
as 
select a.data_source, a."year" , a.uth_member_id ,total_enrolled_months, gender_cd, age_derived , state, plan_type, bus_cd,
       uth_claim_id , claim_type, total_charge_amount , total_allowed_amount, total_paid_amount--, diabetes_flag, cancer_flag 
from data_warehouse.member_enrollment_yearly a 
    left outer join data_warehouse.claim_header b 
     on a.uth_member_id = b.uth_member_id 
    and a."year" = b."year" 
where a.year between 2012 and 2021
  and a.data_source in ('optz', 'truv','mcrt','mcrn')
distributed by (uth_member_id)
;

alter table tableau.dashboard_1720 owner to uthealth_analyst;

analyze tableau.dashboard_1720;

select count(*) from tableau.dashboard_1720

---

-----diag dashboard
drop table if exists  tableau.diag_dashboard;

---diagnosis
create table tableau.diag_dashboard
with (appendoptimized=true, orientation=column, compresstype=zlib)
as 
select data_source, extract(year from from_date_of_service) as year, count(distinct uth_member_id) as members, count(distinct uth_claim_id) as claims, 
        diag_cd, case when diag_position = 1 then 'Primary' else 'Secondary' end as diag_pos, b.code_description as description 
from data_warehouse.claim_diag a
   join reference_tables.ref_cms_icd_cm_codes b
      on a.diag_cd = b.cd_value 
where extract(year from from_date_of_service) between 2012 and 2021 
and data_source in ('optz','truv','mcrt','mcrn')
group by 1,2,5,6,7
;

alter table tableau.diag_dashboard owner to uthealth_analyst;

analyze tableau.diag_dashboard;


select  sum(b.total_allowed_amount) as total_allowed, 
from tableau.dashboard_1720 b 
where a.data_source = 'optz'
group by a.year 
;

with cte_MY as (
select data_source, year, sum(total_enrolled_months) as MY
from tableau.enrollment_only 
group by 1,2  
) 
select  a.data_source, a.year, b.MY, sum(a.total_allowed_amount) as total_allowed
from tableau.dashboard_1720 a
   join cte_MY b 
  on b.data_source = a.data_source 
 and b.year = a.year 
group by 1,2,3
order by 1,2 
;


---1/18/2022 meeting w/Madhuri table for direct connection 
drop table if exists tableau.enrollment_only;

create table tableau.enrollment_only
with (appendoptimized=true, orientation=column, compresstype=zlib)
as 
select data_source, year, uth_member_id, gender_cd, age_derived, state, plan_type, bus_cd, total_enrolled_months
from data_warehouse.member_enrollment_yearly a 
where a.year between 2012 and 2021
  and a.data_source in ('optz', 'truv','mcrt','mcrn')
distributed by (uth_member_id)
;
  
  
alter table tableau.enrollment_only owner to uthealth_analyst;

analyze tableau.enrollment_only;
  
  
  
---conditions
--select * from data_warehouse.conditions_member_enrollment_yearly
drop table if exists tableau.member_conditions;

create table tableau.member_conditions
with (appendoptimized=true, orientation=column, compresstype=zlib)
as 
select a.data_source, a."year" , a.uth_member_id , a.total_enrolled_months , a.gender_cd, a.race_cd, a.age_derived , a.state , a.bus_cd,
      a.aimm, a.ami,  a.ca,      a.cfib, a.chf, a.ckd, a.cliv, a.copd,   a.cysf, 
        a.dem, a.dep, a.epi, a.fbm, a.hemo, a.hep,  a.hiv, a.ihd,  a.lbp, 
      a.lymp, a.ms, a.nicu, a.pain, a.park, a.pneu,a.ra, a.scd, a.smi, a.str, a.tbi, a.trans, a.trau
from data_warehouse.conditions_member_enrollment_yearly a 
where a.year between 2012 and 2021
  and a.data_source in ('optz', 'truv','mcrt','mcrn')
distributed by (uth_member_id)
;
  
  
alter table tableau.member_conditions owner to uthealth_analyst;

analyze tableau.member_conditions;
  
----conditions pivoted
select * 
from conditions.member_enrollment_yearly 
;


select * 
from conditions.person_prof 
;