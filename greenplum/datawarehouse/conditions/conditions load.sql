---logic for condition 
select distinct cd_type from conditions.codeset

----keep first date of service so we can determine what fiscal year the condition first began in-----


---move REF.dbo.NDC_TIER

---carry forward logic
select * from conditions.condition_desc cd where condition_cd = 'CHF' --carryforward yes

select * from conditions.codeset where condition_cd = 'CHF'  --icd9 and icd10 diag 

--diag  
create table conditions.carry_forward_work 
with (appendonly=true, orientation = column) 
as
	select data_source, date as dx_date, uth_member_id,'CHF' as condition_cd 
	from data_warehouse.claim_diag cd 
	where diag_cd in (select cd_value 
	                  from conditions.codeset 
	                  where condition_cd = 'CHF' and cd_type in ('ICD-9','ICD-10')
	                  and position('%' in cd_value) = 0) 
distributed by (uth_member_id);

--diag like
insert into conditions.carry_forward_work 
select data_source, date as dx_date, uth_member_id,'CHF' as condition_cd 
from data_warehouse.claim_diag a 
  join ( select cd_value 
	     from conditions.codeset 
	     where condition_cd = 'CHF' and cd_type in ('ICD-9','ICD-10')
	     and position('%' in cd_value) > 0) b 
     on a.diag_cd like b.cd_value
;

insert into conditions.member_conditions
select  data_source, extract(year from min(dx_date)) as yr, uth_member_id, condition_cd, min(dx_date) as first_date
from conditions.carry_forward_work 
group by data_source, uth_member_id, condition_cd 
;


--carry-forward
select distinct a.data_source , a.year, a.uth_member_id , b.condition_cd , b.first_date
into conditions.carry_forward_load
from data_warehouse.member_enrollment_yearly a 
   join conditions.member_conditions b  
     on b.uth_member_id = a.uth_member_id 
    and b.year < a.year 
    and b.condition_cd = 'CHF'
    order by a.uth_member_id 
;

--load carry forward
insert into conditions.member_conditions 
select a.* 
from conditions.carry_forward_load a 
 left outer join conditions.member_conditions b 
    on b.uth_member_id = a.uth_member_id 
   and a.year = b.year 
where b.uth_member_id is null 
;

select * from conditions.member_conditions where uth_member_id = 100000249

--cleanup
drop table conditions.carry_forward_work;

drop table conditions.carry_forward_load;
--***/end CHF***


---***AMI***
select * from conditions.condition_desc cd where condition_cd = 'AMI' --carryforward no

select * from conditions.codeset where condition_cd = 'AMI'  --icd9 and icd10 diag 

--diag  
create table conditions.measurement_year_work
with (appendonly=true, orientation = column) 
as
	select data_source, year, uth_member_id,'AMI' as condition_cd 
	from data_warehouse.claim_diag cd 
	where diag_cd in (select cd_value 
	                  from conditions.codeset 
	                  where condition_cd = 'AMI' and cd_type in ('ICD-9','ICD-10')
	                  and position('x' in cd_value_raw) = 0) 
distributed by (uth_member_id);

--diag like
insert into conditions.measurement_year_work
select data_source, year, uth_member_id, 'AMI'
from data_warehouse.claim_diag a 
  join ( select cd_value 
	     from conditions.codeset 
	     where condition_cd = 'AMI' and cd_type in ('ICD-9','ICD-10')
	     and position('x' in cd_value_raw) > 0) b 
     on a.diag_cd like b.cd_value
;

insert into conditions.member_conditions
select distinct data_source, year, uth_member_id, condition_cd 
from conditions.measurement_year_work
;


---***/end AMI***


---scratch
select * 
from conditions.member_conditions
order by uth_member_id, year 



select a.year, gender_cd , age_derived , b.condition_cd , count(*) as CHF_Count
from data_warehouse.member_enrollment_yearly a 
   join conditions.member_conditions b 
     on a.uth_member_id = b.uth_member_id
    and a.year = b.year 
   -- and b.condition_cd = 'CHF'
where a.data_source = 'mcrt'
  and a.year between 2014 and 2015
  and a.age_derived = 50
group by a.year, gender_cd , age_derived, b.condition_cd
order by a.year, gender_cd , age_derived, b.condition_cd
;





--- script 


select * 
from conditions.codeset c 
where c.condition_cd = 'CA'