drop table dev.wc_self_harm

create table dev.wc_self_harm (cd text, diag_cd text, diag_desc text, cd_version text);




drop table dev.wc_self_harm_claim_diag

select distinct uth_member_id , year 
into dev.wc_self_harm_claim_diag
from data_warehouse.claim_diag d
where data_source = 'optd'
and diag_cd in ( select diag_cd from dev.wc_self_harm)
and d."year" between 2016 and 2019
;

select * from dev.wc_self_harm-- where diag_cd = 'R45';




-- R45
-- T14




select count(*) from dev.wc_self_harm


select b.* 
into dev.wc_self_harm_cohort
from dev.wc_self_harm_claim_diag a 
  join data_warehouse.member_enrollment_yearly b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year 
   and b.age_derived between 10 and 24 ;
   
---for spreadsheet 
  select year, age_derived, '', count(*)
  from dev.wc_self_harm_cohort
  group by year, age_derived
  order by year, age_derived; 
  
 

 
 ---R45851 Suicidial Ideation
 select distinct a.uth_member_id 
 into dev.wc_self_harm_ideation
 from data_warehouse.claim_diag a 
   join dev.wc_self_harm_cohort b 
     on a.uth_member_id = b.uth_member_id 
 where a.year between 2016 and 2019 
   and a.diag_cd = 'R45851'
   ;
 
---ideation export
select * 
into dev.wc_self_harm_ideation_cohort
from dev.wc_self_harm_cohort a 
where a.uth_member_id in ( select uth_member_id from dev.wc_self_harm_ideation)
;
  
  
 ---T1491 suicide attempt
 select distinct a.uth_member_id 
 into dev.wc_self_harm_attempt
 from data_warehouse.claim_diag a 
   join dev.wc_self_harm_cohort b 
     on a.uth_member_id = b.uth_member_id 
 where a.year between 2016 and 2019 
   and a.diag_cd = 'T1491'
   ;
  
  
---attempt export
select * 
into dev.wc_self_harm_attempt_cohort
from dev.wc_self_harm_cohort a 
where a.uth_member_id in ( select uth_member_id from dev.wc_self_harm_attempt)
;


---for spreadsheet 
  select year, age_derived, '', count(*), count(distinct uth_member_id)
  from dev.wc_self_harm_attempt_cohort
  group by year, age_derived
  order by year, age_derived; 
  
 
---for spreadsheet 
  select year, age_derived, '', count(*), count(distinct uth_member_id)
  from dev.wc_self_harm_ideation_cohort
  group by year, age_derived
  order by year, age_derived; 
 
 
 ---total enrollment
 select count(*), year, age_derived 
 from data_warehouse.member_enrollment_yearly 
 where data_source = 'optd'
 and year between 2016 and 2019
 and age_derived between 10 and 24
  group by year, age_derived
  order by year, age_derived; 
 
