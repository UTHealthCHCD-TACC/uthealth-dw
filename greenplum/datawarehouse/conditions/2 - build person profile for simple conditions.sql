/* ******************************************************************************************************
 *  Build person profile table which has 1 record per member per condition per year 
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * wc001   || 11/18/2021 || script created
 * ******************************************************************************************************
 */



-----*******************************************************************************************************************************************
--** check for various code types by year 

---drop and rebuild table__-------------------------------*
drop table if exists conditions.person_profile_work_table;

create table conditions.person_profile_work_table 
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
insert into conditions.person_profile_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'DX', carry_forward
		from data_warehouse.claim_diag d 
		   join cond_cte cte
         on d.diag_cd = cte.cd_value
        ;

--dx wildcard
drop table conditions.diagnosis_codes_list
       
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
       

insert into conditions.person_profile_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'DX', carry_forward
		from data_warehouse.claim_diag d 
		   join conditions.diagnosis_codes_list c
         on d.diag_cd = c.diag_cd
        ;
              
       
---icd proc
 with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('ICD10-CM','ICD9-CM')
) 		
insert into conditions.person_profile_work_table 
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
insert into conditions.person_profile_work_table 
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
insert into conditions.person_profile_work_table 
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
insert into conditions.person_profile_work_table 
		select d.data_source, d.year, d.uth_member_id, condition_cd, 'drg', carry_forward
		from data_warehouse.claim_detail d 
		   join cond_cte cte
         on d.drg_cd = cte.cd_value
        ;   
       

analyze conditions.person_profile_work_table;




---consolidate 
drop table conditions.person_prof ;

create table conditions.person_prof 
with (appendonly=true, orientation=column, compresstype=zlib) as
	select distinct data_source, year, uth_member_id, condition_cd, carry_forward
	from conditions.person_profile_work_table
distributed by (uth_member_id); 


analyze conditions.person_prof ;


select * 
from conditions.person_prof ;

