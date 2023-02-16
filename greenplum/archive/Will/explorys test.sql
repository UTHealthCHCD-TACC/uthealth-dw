



select count(distinct explorys_patient_id) -- icd_code , replace(icd_code,'.',''), *
from Explorys.dbo.v_diagnosis d 
where replace(icd_code,'.','') like '250%' 
   or replace(icd_code,'.','') like '362%' 
   or replace(icd_code,'.','') in  ('3572','36641')
   or replace(icd_code,'.','') like 'E10%'  
   or replace(icd_code,'.','') like 'E11%' 
   or replace(icd_code,'.','') like 'E13%'
   
   
   
select * 
from Explorys.dbo.v_diagnosis ;


select d.explorys_patient_id  
into stage.dbo.wc_hivdiab_cohort 
from Explorys.dbo.temp_diab d
  join Explorys.dbo.temp_hiv h 
     on d.explorys_patient_id  = h.explorys_patient_id ;
     
    
select a.*
into STAGE.dbo.wc_explorys_admit
from Explorys.dbo.v_admission a 
  join stage.dbo.wc_hivdiab_cohort b 
     on a.explorys_patient_id = b.explorys_patient_id 
;

select a.*
into STAGE.dbo.wc_explorys_demo
from Explorys.dbo.v_demographic a 
  join stage.dbo.wc_hivdiab_cohort b 
     on a.explorys_patient_id = b.explorys_patient_id 
; 


select a.*
into STAGE.dbo.wc_explorys_enc 
from Explorys.dbo.v_encounter a 
  join stage.dbo.wc_hivdiab_cohort b 
     on a.explorys_patient_id = b.explorys_patient_id 
; 


select a.*
into STAGE.dbo.wc_explorys_med
from Explorys.dbo.v_medical_history  a 
  join stage.dbo.wc_hivdiab_cohort b 
     on a.explorys_patient_id = b.explorys_patient_id 
;

select a.*
into STAGE.dbo.wc_explorys_obs
from Explorys.dbo.v_observation a 
  join stage.dbo.wc_hivdiab_cohort b 
     on a.explorys_patient_id = b.explorys_patient_id 
; 


select a.*
into STAGE.dbo.wc_explorys_proc
from Explorys.dbo.v_procedure  a 
  join stage.dbo.wc_hivdiab_cohort b 
     on a.explorys_patient_id = b.explorys_patient_id 
;







