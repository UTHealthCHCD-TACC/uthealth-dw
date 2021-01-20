---logic for condition 


select * from  conditions.condition_desc b 


--diag, no wildcards
with cond_cte as 
( 
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a 
  join conditions.condition_desc b 
    on b.condition_cd = a.condition_cd 
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
    on b.condition_cd = a.condition_cd 
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
where carry_forward = 'N'
;


--carry-forward
insert into conditions.member_conditions
select distinct enrl.data_source , enrl.year, enrl.uth_member_id , wrk.condition_cd 
from data_warehouse.member_enrollment_yearly enrl
   join conditions.diagnosis_work_table wrk 
     on enrl.uth_member_id = wrk.uth_member_id 
    and enrl.year >= wrk.year 
    and wrk.carry_forward = 'Y'
;

---verify
select a.*, b.carry_forward, b.condition_desc 
from conditions.member_conditions a 
   join conditions.condition_desc b 
     on a.condition_cd = b.condition_cd 
where uth_member_id = 100000249
order by condition_cd , year 
;



select * 
from conditions.codeset c where c.condition_cd = 'CHF';




