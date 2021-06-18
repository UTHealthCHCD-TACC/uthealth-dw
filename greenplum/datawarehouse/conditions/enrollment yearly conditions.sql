---***adding conditions to enrollment***

--copy of table
create table conditions.member_enrollment_yearly
with (appendoptimized=false)
as
select * 
from data_warehouse.member_enrollment_yearly
distributed by (uth_member_id)
;


select condition_cd, carry_forward, condition_desc , replace(condition_desc,' ','_') as colname
into dev.wc_cond_desc_temp
from conditions.condition_desc 
where diag_flag = '1' and additional_logic_flag = '0'
and icd_proc_flag is null and drg_flag is null and rev_flag is null and ahfs_flag is null and cpt_hcpcs_flag is null 
order by condition_cd;


---add  conditions using colname result above 
alter table conditions.member_enrollment_yearly add column Trauma_and_Traumatic_Amputation char(1) default '0';


select * from conditions.member_enrollment_yearly



----load 
select * 
from conditions.codeset a 
   join  dev.wc_cond_desc_temp b 
     on a.condition_cd = b.condition_cd 
;



drop table if exists conditions.diagnosis_work_table;

--diag, no wildcards
with cond_cte as 
( 
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a 
   join  dev.wc_cond_desc_temp b 
     on a.condition_cd = b.condition_cd 
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
  join  dev.wc_cond_desc_temp b 
     on a.condition_cd = b.condition_cd 
   and position('%' in a.cd_value) > 0 
)
insert into conditions.diagnosis_work_table
select d.data_source, d.year, d.uth_member_id, c.condition_cd, c.carry_forward
from data_warehouse.claim_diag d 
  join cond_cte c 
    on d.diag_cd like c.cd_value
;

analyze conditions.diagnosis_work_table

---consolidate diagwork into condition 
drop table conditions.person_prof ;

select distinct data_source, year, uth_member_id, condition_cd, carry_forward
into conditions.person_prof 
from conditions.diagnosis_work_table;


analyze conditions.person_prof ;

select distinct condition_cd
from conditions.person_prof 
where carry_forward = '0'
;


select distinct condition_cd , carry_forward
from conditions.person_prof 
;

select * from conditions.member_enrollment_yearly;

--auto_immune_diseases aimm 1 
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'AIMM'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set auto_immune_diseases = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;

--ami 0 
update conditions.member_enrollment_yearly a set ami = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'AMI'
;

--breast_cancer CA-B 0
update conditions.member_enrollment_yearly a set breast_cancer = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'CA-B'
;



--colon cancer 0
update conditions.member_enrollment_yearly a set colon_cancer = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'CA-Co'
;


--cervical cancer 0
update conditions.member_enrollment_yearly a set colon_cancer = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'CA-Cv'
;


--lung cancer 0
update conditions.member_enrollment_yearly a set lung_cancer = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'CA-L'
;


--prostate cancer 0
update conditions.member_enrollment_yearly a set prostate_cancer = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'CA-P'
;


--cystic fybrosis cfib 1
update conditions.member_enrollment_yearly a set cystic_fibrosis = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;



--chronic heart failure CHF  1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'CHF'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set chronic_heart_failure = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--esrd_and_ckd CKD 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'CKD'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set esrd_and_ckd = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--chronic liver dis CLIV 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'CLIV'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set chronic_liver_disease = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;



--COPD 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'COPD'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set COPD = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--cystic fibrosis and resperitory  CRES 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'CRES'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set cystic_fibrosis = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--major depression DEP 0 
update conditions.member_enrollment_yearly a set major_depression = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'DEP'
;


--epilepsy EPI 1 
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'EPI'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set epilepsy = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--fibromyalgia FBM 1 
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'FBM'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set fibromyalgia = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;



--hemophilia HEMO 1 
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'HEMO'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set hemophilia = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--acute_hepatitis_a_or_c HEP 0
update conditions.member_enrollment_yearly a set acute_hepatitis_a_or_c = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'HEP'
;


--homeless HML 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'HML'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set homeless = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--low_back_pain LBP 0
update conditions.member_enrollment_yearly a set low_back_pain = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'LBP'
;

--multiple_sclerosis MS 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'MS'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set multiple_sclerosis = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;



--chronic_pain PAIN 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'PAIN'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set chronic_pain = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--parkinson PARK 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'PARK'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set parkisons = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--pneumonia PNEU 0
update conditions.member_enrollment_yearly a set pneumonia = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'PNEU'
;



--rheumatoid_arthritis RA 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'RA'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set rheumatoid_arthritis = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--sickle_cell_disease SCD 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'SCD'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set sickle_cell_disease = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--schizophrenia SCZ 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'SCZ'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set schizophrenia = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 ;


--chronic_serious_mental_illnesses SMI 1
with carry_cte as (select min(year) as yr, uth_member_id, data_source 
                from conditions.person_prof 
                where condition_cd = 'SMI'
                group by uth_member_id , data_source 
               )
update conditions.member_enrollment_yearly a set chronic_serious_mental_illnesses = '1' 
from carry_cte b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" >= b.yr 
      and a.data_source = b.data_source
 
;


--stroke STR 0
update conditions.member_enrollment_yearly a set stroke = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'STR'
;


--traumatic_brain_injury TBI 0
update conditions.member_enrollment_yearly a set traumatic_brain_injury = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'TBI'
;

--trauma_and_traumatic_amputation TRAU 0
update conditions.member_enrollment_yearly a set trauma_and_traumatic_amputation = '1' 
from conditions.person_prof b 
    where a.uth_member_id = b.uth_member_id 
      and a."year" = b.year 
      and a.data_source = b.data_source
      and b.condition_cd = 'TRAU'
;


