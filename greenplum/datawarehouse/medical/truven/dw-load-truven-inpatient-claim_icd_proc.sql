
/* ******************************************************************************************************
 *  load claim icd proc for truven
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001  || 1/3/2022  ||  removed year, fiscal_year, icd_type
 * ****************************************************************************************************** 
 * */

-----commercial 
insert into dw_staging.claim_icd_proc ( data_source, 
                                        uth_member_id,	
                                        uth_claim_id,
									    claim_sequence_number,
										from_date_of_service,
										proc_cd,
										proc_position,
										icd_version 
									   )								
select * from 
	(        select  'truv', 
	         b.uth_member_id, 
	         b.uth_claim_id, 
	         null as claim_sequence_number, 
	         a.svcdate ,
	         unnest(array[proc1, proc2, proc3, proc4, proc5, proc6])  as proc_cd,
			 unnest(array[1,2,3,4,5,6]) as proc_pos,
			 a.dxver
	from truven.ccaef a
	   join data_warehouse.dim_uth_claim_id b  
	      on a.member_id_src = b.member_id_src 
	     and b.claim_id_src = a.msclmid::text 
 ) inr 
 where inr.proc_cd is not null 
;

 

----med
insert into dw_staging.claim_icd_proc ( data_source, 
                                        uth_member_id,	
                                        uth_claim_id,
									    claim_sequence_number,
										from_date_of_service,
										proc_cd,
										proc_position,
										icd_version 
									   )									   
select * from 
	(  select  'truv', 
         b.uth_member_id, 
         b.uth_claim_id, 
         null as claim_sequence_number, 
         a.svcdate ,
         unnest(array[proc1, proc2, proc3, proc4, proc5, proc6])  as proc_cd,
		 unnest(array[1,2,3,4,5,6]) as proc_pos,
		 a.dxver
from truven.mdcrf a
   join data_warehouse.dim_uth_claim_id b  
      on a.member_id_src = b.member_id_src 
     and b.claim_id_src = a.msclmid::text 
 ) inr 
 where inr.proc_cd is not null 
;     




select * 
from dw_staging.claim_icd_proc 
where data_source = 'truv'
  and uth_claim_id = 36289087943;

 select * 
 from data_warehouse.dim_uth_claim_id b 
 where uth_claim_id = 36289087943;


 select  'truv', 
	         b.uth_member_id, 
	         b.uth_claim_id, 
	         null as claim_sequence_number, 
	         a.svcdate ,
	         unnest(array[proc1, proc2, proc3, proc4, proc5, proc6])  as proc_cd,
			 unnest(array[1,2,3,4,5,6]) as proc_pos,
			 a.dxver
	from truven.ccaef a
	   join data_warehouse.dim_uth_claim_id b  
	      on a.member_id_src = b.member_id_src 
	     and b.claim_id_src = a.msclmid::text 
	     and b.uth_claim_id = 36289087943

	     
	     select * 
	     from data_warehouse.dim_uth_claim_id duci where uth_claim_id = 36289087943
	     
	     select * 
	     from truven.ccaef where msclmid  = '637982668'
	     and enrolid = 27258549101