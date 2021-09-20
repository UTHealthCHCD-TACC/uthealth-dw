/* ******************************************************************************************************
 *  Create condition tables
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 1/1/2019 || script created
 * ******************************************************************************************************
 */

---logic for condition 


select * 
from conditions.condition_desc 
order by condition_cd  
;


drop table if exists conditions.diagnosis_work_table;

--diag, no wildcards
with cond_cte as 
( 
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a 
  join conditions.condition_desc b 
    on b.condition_cd in ('CA-L','COPD') 
   and b.condition_cd = a.condition_cd 
   and b.diag_flag = '1' 
   and position('%' in a.cd_value) = 0   
)
select d.data_source, d.year, d.uth_member_id, c.condition_cd, c.carry_forward
 into conditions.diagnosis_work_table
from data_warehouse.claim_diag d 
  join cond_cte c 
    on c.cd_value = d.diag_cd 
;

--diag wildcards
with cond_cte as 
( 
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a 
  join conditions.condition_desc b 
     on b.condition_cd in ('CA-L','COPD') 
   and b.condition_cd = a.condition_cd 
   and b.diag_flag = '1' 
   and position('%' in a.cd_value) > 0 
)
insert into conditions.diagnosis_work_table
select d.data_source, d.year, d.uth_member_id, c.condition_cd, c.carry_forward
from data_warehouse.claim_diag d 
  join cond_cte c 
    on d.diag_cd like c.cd_value
;


select * from conditions.diagnosis_work_table

--insert conditions that are measured yearly
insert into conditions.member_conditions
select distinct data_source, year, uth_member_id, condition_cd 
from conditions.diagnosis_work_table 
where carry_forward = '0'
;


--carry-forward
insert into conditions.member_conditions
select distinct enrl.data_source , enrl.year, enrl.uth_member_id , wrk.condition_cd 
from data_warehouse.member_enrollment_yearly enrl
   join conditions.diagnosis_work_table wrk 
     on enrl.uth_member_id = wrk.uth_member_id 
    and enrl.year >= wrk.year 
    and wrk.carry_forward = '1'
;



select count(*), count(distinct uth_member_id), year 
from conditions.diagnosis_work_table 
order by uth_member_id , year 


select * 
from conditions.member_conditions where condition_cd = 'CA-L'
order by uth_member_id , year 
;


select count(*), year, plan_type 
from data_warehouse.member_enrollment_yearly mey where data_source = 'mcrt'
group by year, plan_type 
order by year, plan_type 
;

drop table if exists conditions.person_profile;
create table conditions.person_profile ( data_source char(4), year int2, uth_member_id bigint, member_id_src text, age_derived int2,gender_cd char(1), 
                                         CAL int2 default 0, COPD int2 default 0
                                       );
                                      
                                      
insert into conditions.person_profile 
select a.data_source, a."year", a.uth_member_id, b.member_id_src, a.age_derived , a.gender_cd 
from data_warehouse.member_enrollment_yearly a 
  join data_warehouse.dim_uth_member_id b 
    on a.uth_member_id = b.uth_member_id 
   and a.data_source = b.data_source 
where a.data_source = 'mcrt'
;


update conditions.person_profile a set CAL = 1
from conditions.member_conditions b 
where b.uth_member_id = a.uth_member_id 
  and b.year = a.year
  and b.data_source = a.data_source
  and b.condition_cd = 'CA-L'
;  


update conditions.person_profile a set COPD = 1
from conditions.member_conditions b 
where b.uth_member_id = a.uth_member_id 
  and b.year = a.year
  and b.data_source = a.data_source
  and b.condition_cd = 'COPD'
;  

select * from conditions.person_profile;

select * 
from conditions.codeset c where condition_cd = 'COPD';


select count(*), year  
from conditions.person_profile
where  copd = 1
group by year 
order by year 
;


  




