/* ******************************************************************************************************
 *  Adding conditions to enrollment
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 1/1/2019 || script created
 * ******************************************************************************************************
 */

---***adding conditions to enrollment***

drop table if exists conditions.member_enrollment_yearly;

--copy of table
create table conditions.member_enrollment_yearly
with (appendoptimized=true, orientation=column, compresstype=zlib)
as
select * 
from data_warehouse.member_enrollment_yearly
distributed by (uth_member_id)
;


vacuum analyze conditions.member_enrollment_yearly ;


---function to create one column in yearly enrollment for each condition 
create or replace function conditions.cond_columns(_schm text, _tbl text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
as $$ 
declare
	r_cond_cd text;
begin
	
for r_cond_cd 
	 in 
		select condition_cd 
		from conditions.condition_desc cd 		
	loop 	
		if exists ( select 1 
	                from information_schema.columns 
	                where table_schema = _schm
	               	  and table_name = _tbl
	                  and column_name = r_cond_cd)
	        then
	        raise notice 'column % already exists', r_cond_cd;	
		else 
			
		   execute format ('alter table %s.%s add column %s char(1)', _schm, _tbl, r_cond_cd);
		   raise notice 'added:  %.% add column % ', _schm, _tbl, r_cond_cd;
		end if;
	end loop;
	
end 
$$ EXECUTE ON ANY;

---run function 
select conditions.cond_columns('conditions','member_enrollment_yearly');

---verify
select * from conditions.member_enrollment_yearly;


-----*******************************************************************************************************************************************
--** function to populate diagnosis conditions
--------------------

---drop and rebuild table__-------------------------------*
drop table if exists conditions.diagnosis_work_table;

create table conditions.diagnosis_work_table 
(
	data_source char(4),
	year int2, 
	uth_member_id bigint, 
	condition_cd text, 
	cd_type text,
	carry_forward char(1)
)
with (appendonly=true, orientation=column, compresstype=zlib) 
distributed by (uth_member_id); 
----------------------------------------------------------*

--ICD10-CM = icd procedure code 
--ICD-10 = diagnosis codes
--CPT = cpt/hcpcs

--dx exact
with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('ICD-10','ICD-9')
) 		
insert into conditions.diagnosis_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'DX', carry_forward
		from data_warehouse.claim_diag d 
		   join cond_cte cte
         on d.diag_cd = cte.cd_value
        ;


create table dev.wc_all_diagnosis_codes as 
select distinct diag_cd 
from data_warehouse.claim_diag cd 
;       
  

 with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
	where position('%' in a.cd_value) > 0
	  and a.cd_type in ('ICD-10','ICD-9')
) 	
select d.diag_cd, c.condition_cd, c.carry_forward
   into conditions.diagnosis_codes_list
from dev.wc_all_diagnosis_codes d 
  join cond_cte c 
    on d.diag_cd like c.cd_value
;      
       
--dx wildcard
insert into conditions.diagnosis_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'DX', carry_forward
		from data_warehouse.claim_diag d 
		   join conditions.diagnosis_codes_list c
         on d.diag_cd = c.diag_cd
        ;
              
       
---icd proc exact 
 with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('ICD10-CM','ICD9-CM')
) 		
insert into conditions.diagnosis_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'proc', carry_forward
		from data_warehouse.claim_icd_proc d 
		   join cond_cte cte
         on d.proc_cd  = cte.cd_value
        ;

       
--cpt hcpcs
 with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('CPT')
) 		
insert into conditions.diagnosis_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'cpt', carry_forward
		from data_warehouse.claim_detail d 
		   join cond_cte cte
         on d.cpt_hcpcs_cd = cte.cd_value
        ;    

--rev
 with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('REV')
) 		
insert into conditions.diagnosis_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'rev', carry_forward
		from data_warehouse.claim_detail d 
		   join cond_cte cte
         on d.revenue_cd = cte.cd_value
        ;   
       
--drg
 with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('DRG')
) 		
insert into conditions.diagnosis_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'drg', carry_forward
		from data_warehouse.claim_detail d 
		   join cond_cte cte
         on d.drg_cd = cte.cd_value
        ;   
       
       
-----------------------------
analyze conditions.diagnosis_work_table;



----------------------------------------------------------------------------------------------------------------


---consolidate diagwork into condition 
drop table conditions.person_prof ;

create table conditions.person_prof 
with (appendonly=true, orientation=column, compresstype=zlib) as
	select distinct data_source, year, uth_member_id, condition_cd, carry_forward
	from conditions.diagnosis_work_table
distributed by (uth_member_id); 



analyze conditions.person_prof ;

select * 
from conditions.person_prof 



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


select year, count(*) 
from conditions.member_enrollment_yearly 
where copd = '1' and data_source = 'truv' and bus_cd = 'COM' 
group by year 
order by year; 



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


