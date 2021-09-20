/* ******************************************************************************************************
 *  Condition related queries
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 1/1/2019 || script created
 * ******************************************************************************************************
 */

----   wc 6/10/2021 hbtm


---*** codeset table is provided by jeff based on criteria from dr krause, imported and modify code values to be compatible with postgres logic statements
---codes used to identify conditions, dx, proc, rev code etc
drop table conditions.codeset 

create table conditions.codeset ( log_seq int2, condition_cd text, cd_type text,  cd_value_raw text, cd_value text );

update conditions.codeset set cd_value = replace(replace(cd_value_raw,'.',''),'x','%')

select * from conditions.codeset c where cd_value like '%\%\%%'

update conditions.codeset set cd_value = '6820%' where cd_value_raw = '6820xx'

select * from conditions.codeset cd 

select * from conditions.condition_desc cd 


---*** condition desc table indicates if a condition if carry forward and also what types of codes are part of the criteria set. 
--- if there is logic behind simply finding one occurence of the code in the measurement year then the additional logic flag will be true
--- check condition logic spraedsheet in shared drive for in-depth logic on these


---create table 
drop table conditions.condition_desc;

create table conditions.condition_desc ( condition_cd text, condition_desc text, type_cd text, type_desc text, carry_forward char(1),
                                         diag_flag char(1), icd_proc_flag char(1), drg_flag char(1), rev_flag char(1), ahfs_flag char(1), cpt_hcpcs_flag char(1),
                                         additional_logic_flag char(1)
                                       );

  
                                      
select * from conditions.condition_desc order by condition_cd;

update conditions.condition_desc set carry_forward = '1' where carry_forward = 'Y';

update conditions.condition_desc set carry_forward = '0' where carry_forward = 'N';

alter table conditions.condition_desc add column logic_version text default 'v36';

alter table conditions.condition_desc add column additional_logic_flag char(1) default '0';



update conditions.condition_desc set additional_logic_flag = '1' where condition_cd in ('ASTH','DB','DEL','DEM','HTN','LB','LBPREG','OPI','PREG','TOB');
                                      

select distinct c.cd_type , a.condition_cd 
into dev.wc_cond_desc_flags
from conditions.condition_desc a 
  join conditions.codeset c 
    on c.condition_cd = a.condition_cd 
;


select distinct condition_cd , cd_type from dev.wc_cond_desc_flags order by condition_cd , cd_type ;

--diag
with condcte as
(   select distinct condition_cd 
	from dev.wc_cond_desc_flags 
	where cd_type in ('ICD-10','ICD-9') 
)
update conditions.condition_desc a set diag_flag = '1' 
from condcte b
where a.condition_cd = b.condition_cd
 ; 

--icd proc
with condcte as
(   select distinct condition_cd 
	from dev.wc_cond_desc_flags 
	where cd_type in ('ICD10-CM','ICD9-CM') 
)
update conditions.condition_desc a set icd_proc_flag = '1' 
from condcte b
where a.condition_cd = b.condition_cd
 ; 

--drg
with condcte as
(   select distinct condition_cd 
	from dev.wc_cond_desc_flags 
	where cd_type in ('DRG') 
)
update conditions.condition_desc a set drg_flag = '1' 
from condcte b
where a.condition_cd = b.condition_cd
 ; 

 --rev
with condcte as
(   select distinct condition_cd 
	from dev.wc_cond_desc_flags 
	where cd_type in ('REV') 
)
update conditions.condition_desc a set rev_flag = '1' 
from condcte b
where a.condition_cd = b.condition_cd
 ;    
    
 --ahfs (ther class)
with condcte as
(   select distinct condition_cd 
	from dev.wc_cond_desc_flags 
	where cd_type in ('AHFS') 
)
update conditions.condition_desc a set ahfs_flag = '1' 
from condcte b
where a.condition_cd = b.condition_cd
;  

    
 --cpt_hcpcs
with condcte as
(   select distinct condition_cd 
	from dev.wc_cond_desc_flags 
	where cd_type in ('CPT') 
)
update conditions.condition_desc a set cpt_hcpcs_flag = '1' 
from condcte b
where a.condition_cd = b.condition_cd
 ;  


-- end 
select * from conditions.condition_desc cd ;









