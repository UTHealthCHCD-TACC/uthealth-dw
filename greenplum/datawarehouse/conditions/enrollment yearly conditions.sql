---***adding conditions to enrollment***

--copy of table
create table dw_qa.wc_member_enrollment_yearly
with (appendoptimized=false)
as
select * 
from data_warehouse.member_enrollment_yearly
distributed by (uth_member_id)
;


---add test conditions
alter table dw_qa.wc_member_enrollment_yearly add column CA_L char(1) default '0';

alter table dw_qa.wc_member_enrollment_yearly add column COPD char(1) default '0';


vacuum analyze dw_qa.wc_member_enrollment_yearly;

----load 


select count(*), count(distinct uth_member_id), year 
from conditions.member_conditions
group by year;


--lung cancer
update dw_qa.wc_member_enrollment_yearly a set ca_l = '1' 
from conditions.member_conditions b  
where a.uth_member_id = b.uth_member_id 
  and a."year" = b."year"
  and b.condition_cd = 'CA-L'
;



--lung cancer
update dw_qa.wc_member_enrollment_yearly a set copd = '1' 
from conditions.member_conditions b  
where a.uth_member_id = b.uth_member_id 
  and a."year" = b."year"
  and b.condition_cd = 'COPD'
;


select * from dw_qa.wc_member_enrollment_yearly where uth_member_id = 148363957 order by month_year_id;


--copd = '1';



select * 
from dev.am_member_enrollment_monthly a where claim_created_flag is true;

where uth_member_id = 148363957 order by month_year_id;



select * 
from dev.am_member_enrollment_yearly a 
where uth_member_id = 148363957 order by year











