/* ******************************************************************************************************
 *  Condition related queries
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 1/1/2021   || script created
 * ******************************************************************************************************
 *  wc001        || 11/18/2021 || modified to all lower case
 * ******************************************************************************************************
 */

----   wc 6/10/2021 hbtm


---*** codeset table is provided by jeff based on criteria from dr krause, imported and modify code values to be compatible with postgres logic statements
---codes used to identify conditions, dx, proc, rev code etc
drop table conditions.codeset 

create table conditions.codeset ( log_seq int2, condition_cd text, cd_type text,  cd_value_raw text, cd_value text , logic_version text);

update conditions.codeset set cd_value = replace(replace(cd_value_raw,'.',''),'x','%')

--replaces xx --> %% to %
update conditions.codeset set cd_value = substring(cd_value, 0,  position('%' in cd_value)+1) where cd_value like '%\%\%%'
--asthma in source has incorrect values; they are identified by a negative log_seq
delete from conditions.codeset where log_seq<0

select distinct condition_cd from conditions.codeset cd 

update conditions.condition_desc set logic_version = 'v40'

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

  
insert into conditions.condition_desc (condition_cd, condition_desc, type_cd) values
('ami','Acute Myocardial Infarction','A'),
('hep','Hepatitis','A'),
('pneu','Pneumonia','A'),
('str','Stroke','A'),
('tob','Tobacco Use','A'),
('aimm','Auto Immune Disease','C'),
('asth','Asthma','C'),
('chf','Chronic Heart Failure','C'),
('ckd','Chronic Kidney','C'),
('cliv','Chronic Liver','C'),
('copd','Chronic Obstructive Pulmonary Disease','C'),
('dem','Dementia','C'),
('diab','Diabetes','C'),
('epi','Epilepsy','C'),
('fbm','Fibromyalgia','C'),
('hemo','Hemophilia','C'),
('hiv','HIV','C'),
('htn','Hypertension','C'),
('ihd','Ischemic Heart Disease','C'),
('lymp','Lymphedema','C'),
('ms','MS','C'),
('pain','Chronic Pain','C'),
('park','Parkinsons','C'),
('ra','Rheumatoid Arthritis','C'),
('scd','Sickle Cell Disease','C'),
('smi','Serious Mental Illness','C'),
('del','Delivery','E'),
('lbirth','Live Birth','E'),
('lbpreg','Rate of Pregnancy resulting in Live Birth','E'),
('preg','Pregnancy','E'),
('cfib','Cystic Fibrosis','G'),
('cysf','Cystic Fibrosis and other Respiratory Conditions','G'),
('ca','Cancer','P'),
('dep','Depression','P'),
('lbp','Low Back Pain','P'),
('opi','Opioid','P'),
('nicu','NICU','T'),
('tbi','Traumatic Brain Injruy','T'),
('trans','Transplant','T'),
('trau','Trauma ','T')
;
                                      
                                     

update conditions.condition_desc set type_desc = case  
 when type_cd = 'C' then 'Chronic' 
 when type_cd = 'P' then 'Persistent' 
 when type_cd = 'T' then 'Catastrophic'
 when type_cd = 'E' then 'Episodic'
 when type_cd = 'A' then 'Acute'
 when type_cd = 'G' then 'Congenital'
 when type_cd = 'D' then 'Disabled'
 when type_cd = 'PR' then 'Procedure'
 when type_cd = 'H' then 'Historic'
	else type_cd
 end;




alter table conditions.condition_desc add column logic_version text default 'v40';

alter table conditions.condition_desc_new add column additional_logic_flag char(1) default '0';




update conditions.condition_desc set additional_logic_flag = '1' where condition_cd in ('asth','db','del','dem','htm','lb','lbpreg','opi','preg','tob');



drop table if exists dev.wc_cond_desc_flags;

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



---remove dashes and convert to lowercase for consistency across greenplum
update conditions.condition_desc set condition_cd = replace(condition_cd,'-','');

update conditions.codeset set condition_cd = replace(condition_cd,'-','');


update conditions.condition_desc set condition_cd = lower(condition_cd);

update conditions.codeset set condition_cd = lower(condition_cd); 

--create copy in ref for user access 
drop table if exists reference_tables.condition_desc;

create table reference_tables.condition_desc as table conditions.condition_desc ; 


