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
	cd_value text,
	cd_type text,
	condition_cd text,
	carry_forward char(1)
)
with (appendonly=true, orientation=column, compresstype=zlib) 
distributed by (uth_member_id); 
----------------------------------------------------------*

analyze conditions.person_profile_work_table ;

--ICD10-CM = icd procedure code 
--ICD-10 = diagnosis codes
--CPT = cpt/hcpcs



--Diagnosis codes
	
--list will be used to avoid using like condition on insert statement 
drop table conditions.diagnosis_codes_list;  

---wildcards into diagnosis code list
 with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward, b.additional_logic_flag 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
	where position('%' in a.cd_value) > 0
	  and a.cd_type in ('ICD-10','ICD-9')
) 	
select d.cd_value as diag_cd, c.condition_cd, c.carry_forward, additional_logic_flag 
   into conditions.diagnosis_codes_list
from reference_tables.ref_cms_icd_cm_codes d 
  join cond_cte c 
    on d.cd_value like c.cd_value
;     

---non-wildcards into diagnosis code list
insert into conditions.diagnosis_codes_list
select a.cd_value, a.condition_cd, b.carry_forward, b.additional_logic_flag 
from conditions.codeset a
	join  conditions.condition_desc b
	  on a.condition_cd = b.condition_cd 
where position('%' in a.cd_value) = 0
  and a.cd_type in ('ICD-10','ICD-9')
;

---Diagnosis
--pull patient records into work table 
insert into conditions.person_profile_work_table 
		select  d.data_source, extract(year from d.from_date_of_service) as yr, d.uth_member_id, d.diag_cd, 'DX', c.condition_cd, c.carry_forward
		from data_warehouse.claim_diag d 
		   join conditions.diagnosis_codes_list c
         on d.diag_cd = c.diag_cd
         and c.additional_logic_flag = '0'
        ;

       
       
---icd proc
 with cond_cte as 
(
	select a.cd_value, a.condition_cd, b.carry_forward 
	from conditions.codeset a
		join  conditions.condition_desc b
 		  on a.condition_cd = b.condition_cd 
 		 and b.additional_logic_flag = '0'
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('ICD10-CM','ICD9-CM')
) 		
insert into conditions.person_profile_work_table 
		select d.data_source, extract(year from d.from_date_of_service) as yr, d.uth_member_id, cd_value, 'proc', cte.condition_cd, cte.carry_forward
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
 		  	 and b.additional_logic_flag = '0'
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('CPT')
) 		
insert into conditions.person_profile_work_table 
		select d.data_source, extract(year from d.from_date_of_service) as yr, d.uth_member_id, cd_value, 'cpt', cte.condition_cd, cte.carry_forward
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
		 and b.additional_logic_flag = '0'
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('REV')
) 		
insert into conditions.person_profile_work_table 
		select d.data_source, extract(year from d.from_date_of_service) as yr, d.uth_member_id, cd_value, 'rev', cte.condition_cd, cte.carry_forward
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
 		  	 and b.additional_logic_flag = '0'
	where position('%' in a.cd_value) = 0
	  and a.cd_type in ('DRG')
) 		
insert into conditions.person_profile_work_table 
		select d.data_source, extract(year from d.from_date_of_service) as yr, d.uth_member_id, cd_value, 'drg', cte.condition_cd, cte.carry_forward
		from data_warehouse.claim_detail d 
		   join cond_cte cte
         on d.drg_cd = cte.cd_value
        ;   
       

analyze conditions.person_profile_work_table;


select distinct condition_cd from conditions.person_profile_work_table;


---consolidate 
drop table conditions.person_profile_stage ;

create table conditions.person_profile_stage
with (appendonly=true, orientation=column, compresstype=zlib) as
	select distinct data_source, year, uth_member_id, condition_cd, carry_forward
	from conditions.person_profile_work_table
distributed by (uth_member_id); 


analyze conditions.person_profile_stage ;


