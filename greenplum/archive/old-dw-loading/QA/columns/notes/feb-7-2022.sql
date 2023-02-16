select claim_sequence_number 
from dw_staging.claim_detail where data_source = 'truv';

select claim_type from dw_staging.claim_header where data_source = 'mcrn'

select icd_version from dw_staging.claim_diag 
where data_source = 'truv' and from_date_of_service 
between '2010-01-01' and '2011-01-01';


delete from qa_reporting.claim_proc_column_checks 
where test_var = 'icd_version';


select icd_version from dw_staging.claim_icd_proc where icd_version is not null 
and data_source = 'truv';

select dxver from truven.ccaef where dxver is not null;


delete from qa_reporting.claim_detail_column_checks 
where test_var = 'place_of_service';


select place_of_service 
from dw_staging.claim_detail 
where place_of_service !~ '^\d{1,2}$' and place_of_service is not null
/*
|place_of_service|
|----------------|
|1.              |
|3.              |
|1.              |
|1.              |
|1.              |
|1.              |
|1.              |
|3.              |
|3.              |
*/
/*
------ DIAG
-- check icd_version on mcrn 
			--if garret is using that then there may be issues if not transformed 
			
			--poa src 

-- missing uth_member_id in most tables for truven

poa 



------------- detail 

mdcd check those bad admit and discharge claims for mdcd 
				trudy said only check facility claims for those 

check to see if NA is still in optum if so replace with script 

look into place fo service for optum 

check to date of service for optum FAILS 


-------------- monthly enroll 
			fine 
---------- year enroll
		fine 
		
		--------------- claim header 
		
				---------- look at claim_type for medicare 
				
----- proc 
		look into for medicare  
		all truven values are null in source table so idk do we even need the variable 
			
