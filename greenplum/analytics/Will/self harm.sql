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


select count(*) from dev.wc_self_harm


select b.* 
into dev.wc_self_harm_cohort
from dev.wc_self_harm_claim_diag a 
  join data_warehouse.member_enrollment_yearly b 
    on a.uth_member_id = b.uth_member_id 
   and a.year = b.year 
   and b.age_derived between 10 and 24 ;
   
  
  select year, age_derived, '', count(*)
  from dev.wc_self_harm_cohort
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
 
