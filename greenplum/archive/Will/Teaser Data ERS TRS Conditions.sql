---Step Five
--dev.wc_tease_opioid_fills
drop table if exists wrk.dbo.wc_teaser_conditions;

--
select combo_id, a.srv_start_dt as dos, 'TRS' as data_source, 'chronic pain' as cond  
into wrk.dbo.wc_teaser_conditions
from TRSERS.dbo.TRS_CLM_FIN_NEW a
where substring(med_yrmnth,1,4) in ('2018', '2019' , '2020')
and ( 	 REPLACE(a.pri_icd9_dx_cd,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       or REPLACE(a.icd9_dx_cd_2,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       or REPLACE(a.icd9_dx_cd_3,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       or REPLACE(a.icd9_dx_cd_4,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890') 
	       or REPLACE(a.icd9_dx_cd_5,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890') 		
	       or REPLACE(a.icd9_dx_cd_6,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       or REPLACE(a.icd9_dx_cd_7,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       or REPLACE(a.icd9_dx_cd_8,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       or REPLACE(a.icd9_dx_cd_9,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       or REPLACE(a.icd9_dx_cd_10,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       )    	       
  ;
 
 insert into wrk.dbo.wc_teaser_conditions
	select id, cast(a.FirstDateOfService as date) as dos, 'ERS' as ds, 'chronic pain' as cond 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where substring(a.clm_yrmnth,1,4) in ('2018', '2019' , '2020')
	  and ( 
	         replace(a.DiagnosisCode1,'.','')  in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	         or replace(a.DiagnosisCode2,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	         or replace(a.DiagnosisCode3,'.','')  in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	         or replace(a.DiagnosisCode4,'.','')  in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	         or replace(a.DiagnosisCode5,'.','') in ('G8921','G894','G8929','G8928','G8922','G892','F4541','G894','G890')
	       )
;  
 
 
insert into wrk.dbo.wc_teaser_conditions
select combo_id, a.srv_start_dt as dos, 'TRS' as data_source, 'cancer' as cond  
from TRSERS.dbo.TRS_CLM_FIN_NEW a
where substring(med_yrmnth,1,4) in ('2018', '2019' , '2020')
and ( 	 REPLACE(a.pri_icd9_dx_cd,'.','')  in ('Z510','Z511','Z5111','Z5112','Z08')
	       or REPLACE(a.icd9_dx_cd_2,'.','')in ('Z510','Z511','Z5111','Z5112','Z08')
	       or REPLACE(a.icd9_dx_cd_3,'.','') in ('Z510','Z511','Z5111','Z5112','Z08')
	       or REPLACE(a.icd9_dx_cd_4,'.','') in ('Z510','Z511','Z5111','Z5112','Z08')
	       or REPLACE(a.icd9_dx_cd_5,'.','') in ('Z510','Z511','Z5111','Z5112','Z08')		
	       or REPLACE(a.icd9_dx_cd_6,'.','') in ('Z510','Z511','Z5111','Z5112','Z08')
	       or REPLACE(a.icd9_dx_cd_7,'.','') in ('Z510','Z511','Z5111','Z5112','Z08')
	       or REPLACE(a.icd9_dx_cd_8,'.','') in ('Z510','Z511','Z5111','Z5112','Z08')
	       or REPLACE(a.icd9_dx_cd_9,'.','') in ('Z510','Z511','Z5111','Z5112','Z08')
	       or REPLACE(a.icd9_dx_cd_10,'.','') in ('Z510','Z511','Z5111','Z5112','Z08')
	       or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between 'C00'  and    'C14'
	       or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between 'C15'  and    'C26'
	       or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between 'C30'  and    'C39'
	       or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between  'C40'  and    'C41'
	       or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between 'C43'  and    'C44'
	       or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between 'C45'  and    'C49'
	       or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) = 'C50'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between  'C51'  and    'C58'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between  'C60'  and    'C63'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between  'C64'  and    'C68'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between  'C69'  and    'C72'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between 'C73'  and    'C75'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between 'C76'  and    'C80'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) =  'C7A'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) =  'C7B'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between  'C81'  and    'C96'
		or substring( REPLACE(a.pri_icd9_dx_cd,'.',''),1,3) between 'D00'  and  'D09'
	       )    
	       

 insert into wrk.dbo.wc_teaser_conditions
	select id, cast(a.FirstDateOfService as date) as dos, 'ERS' as ds, 'cancer' as cond 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where substring(a.clm_yrmnth,1,4) in ('2018', '2019' , '2020')
	  and  ( 	 REPLACE(a.DiagnosisCode1 ,'.','')  in ('Z510','Z511','Z5111','Z5112','Z08')
	       or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between 'C00'  and    'C14'
	       or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between 'C15'  and    'C26'
	       or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between 'C30'  and    'C39'
	       or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between  'C40'  and    'C41'
	       or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between 'C43'  and    'C44'
	       or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between 'C45'  and    'C49'
	       or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) = 'C50'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between  'C51'  and    'C58'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between  'C60'  and    'C63'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between  'C64'  and    'C68'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between  'C69'  and    'C72'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between 'C73'  and    'C75'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between 'C76'  and    'C80'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) =  'C7A'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) =  'C7B'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between  'C81'  and    'C96'
		or substring( REPLACE(a.DiagnosisCode1,'.',''),1,3) between 'D00'  and  'D09'
	       )    
;  	       
	       
insert into wrk.dbo.wc_teaser_conditions
select combo_id, a.srv_start_dt as dos, 'TRS' as data_source,'palia' as cond  
from TRSERS.dbo.TRS_CLM_FIN_NEW a
where substring(med_yrmnth,1,4) in ('2018', '2019' , '2020')
and ( 	 REPLACE(a.pri_icd9_dx_cd,'.','') = 'Z515'
	       or REPLACE(a.icd9_dx_cd_2,'.','') = 'Z515'
	       or REPLACE(a.icd9_dx_cd_3,'.','') = 'Z515'
	       or REPLACE(a.icd9_dx_cd_4,'.','') = 'Z515'
	       or REPLACE(a.icd9_dx_cd_5,'.','') = 'Z515'
	       or REPLACE(a.icd9_dx_cd_6,'.','') = 'Z515'
	       or REPLACE(a.icd9_dx_cd_7,'.','') = 'Z515'
	       or REPLACE(a.icd9_dx_cd_8,'.','')= 'Z515'
	       or REPLACE(a.icd9_dx_cd_9,'.','')= 'Z515'
	       or REPLACE(a.icd9_dx_cd_10,'.','') = 'Z515'
	       )    	     
  ;
 
 insert into wrk.dbo.wc_teaser_conditions
	select id, cast(a.FirstDateOfService as date) as dos, 'ERS' as ds, 'palia' as cond 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where substring(a.clm_yrmnth,1,4) in ('2018', '2019' , '2020')
	  and ( 
	         replace(a.DiagnosisCode1,'.','')  = 'Z515'
	         or replace(a.DiagnosisCode2,'.','') = 'Z515'
	         or replace(a.DiagnosisCode3,'.','')  = 'Z515'
	         or replace(a.DiagnosisCode4,'.','')  = 'Z515'
	         or replace(a.DiagnosisCode5,'.','') = 'Z515'
	       )
;  
  
 
insert into wrk.dbo.wc_teaser_conditions
select combo_id, a.srv_start_dt as dos, 'TRS' as data_source,  'vascu' as cond  
from TRSERS.dbo.TRS_CLM_FIN_NEW a
where substring(med_yrmnth,1,4) in ('2018', '2019' , '2020')
and ( 	 REPLACE(a.pri_icd9_dx_cd,'.','') in ('D570','D571','D572')
	       or REPLACE(a.icd9_dx_cd_2,'.','') in ('D570','D571','D572')
	       or REPLACE(a.icd9_dx_cd_3,'.','') in ('D570','D571','D572')
	       or REPLACE(a.icd9_dx_cd_4,'.','') in ('D570','D571','D572')
	       or REPLACE(a.icd9_dx_cd_5,'.','') in ('D570','D571','D572')	
	       or REPLACE(a.icd9_dx_cd_6,'.','') in ('D570','D571','D572')
	       or REPLACE(a.icd9_dx_cd_7,'.','') in ('D570','D571','D572')
	       or REPLACE(a.icd9_dx_cd_8,'.','') in ('D570','D571','D572')
	       or REPLACE(a.icd9_dx_cd_9,'.','') in ('D570','D571','D572')
	       or REPLACE(a.icd9_dx_cd_10,'.','') in ('D570','D571','D572')
)
  ;
 
 insert into wrk.dbo.wc_teaser_conditions
	select id, cast(a.FirstDateOfService as date) as dos, 'ERS' as ds, 'vascu' as cond 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where substring(a.clm_yrmnth,1,4) in ('2018', '2019' , '2020')
	  and ( 
	         replace(a.DiagnosisCode1,'.','')  in ('D570','D571','D572')
	         or replace(a.DiagnosisCode2,'.','')in ('D570','D571','D572')
	         or replace(a.DiagnosisCode3,'.','') in ('D570','D571','D572')
	         or replace(a.DiagnosisCode4,'.','')  in ('D570','D571','D572')
	         or replace(a.DiagnosisCode5,'.','')in ('D570','D571','D572')
	       )
;   
 
insert into wrk.dbo.wc_teaser_conditions
select combo_id, a.srv_start_dt as dos, 'TRS' as data_source,  'ms' as cond  
from TRSERS.dbo.TRS_CLM_FIN_NEW a
where substring(med_yrmnth,1,4) in ('2018', '2019' , '2020')
and ( 	 REPLACE(a.pri_icd9_dx_cd,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_2,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_3,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_4,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_5,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_6,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_7,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_8,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_9,'.','') in ( 'G35','M05','M06','M797')
	       or REPLACE(a.icd9_dx_cd_10,'.','') in( 'G35','M05','M06','M797')
	       )

  ; 

  insert into wrk.dbo.wc_teaser_conditions
	select id, cast(a.FirstDateOfService as date) as dos, 'ERS' as ds, 'ms' as cond 
	from TRSERS.dbo.ERS_BCBSMedCLM a
	where substring(a.clm_yrmnth,1,4) in ('2018', '2019' , '2020')
	  and ( 
	         replace(a.DiagnosisCode1,'.','')  in ( 'G35','M05','M06','M797')
	         or replace(a.DiagnosisCode2,'.','')in ( 'G35','M05','M06','M797')
	         or replace(a.DiagnosisCode3,'.','') in ( 'G35','M05','M06','M797')
	         or replace(a.DiagnosisCode4,'.','')  in ( 'G35','M05','M06','M797')
	         or replace(a.DiagnosisCode5,'.','')in ( 'G35','M05','M06','M797')
	       )
;

