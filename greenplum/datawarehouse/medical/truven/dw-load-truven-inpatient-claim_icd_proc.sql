--- Claim icd proc


-----commercial 
insert into dw_staging.claim_icd_proc ( data_source, 
                                        year, 
                                        uth_member_id,	
                                        uth_claim_id,
									    claim_sequence_number,
										from_date_of_service,
										proc_cd,
										proc_position,
										icd_type,
										fiscal_year
									   )								
select * from 
	(        select  'truv', 
	         extract(year from a.svcdate),
	         b.uth_member_id, 
	         b.uth_claim_id, 
	         a.seqnum, 
	         a.svcdate ,
	         unnest(array[proc1, proc2, proc3, proc4, proc5, proc6])  as proc_cd,
			 unnest(array[1,2,3,4,5,6]) as proc_pos,
			 a.dxver, 
			 dev.fiscal_year_func(a.svcdate)
	from truven.ccaef a
	   join data_warehouse.dim_uth_claim_id b  
	      on a.member_id_src = b.member_id_src 
	     and b.claim_id_src = a.msclmid::text 
 ) inr 
 where inr.proc_cd is not null 
;

 

----med
insert into dw_staging.claim_icd_proc ( data_source, 
                                        year, 
                                        uth_member_id,	
                                        uth_claim_id,
									    claim_sequence_number,
										from_date_of_service,
										proc_cd,
										proc_position,
										icd_type,
										fiscal_year
									   )									   
select * from 
	(  select  'truv', 
         extract(year from a.svcdate),
         b.uth_member_id, 
         b.uth_claim_id, 
         a.seqnum, 
         a.svcdate ,
         unnest(array[proc1, proc2, proc3, proc4, proc5, proc6])  as proc_cd,
		 unnest(array[1,2,3,4,5,6]) as proc_pos,
		 a.dxver, 
		 dev.fiscal_year_func(a.svcdate)
from truven.mdcrf a
   join data_warehouse.dim_uth_claim_id b  
      on a.member_id_src = b.member_id_src 
     and b.claim_id_src = a.msclmid::text 
 ) inr 
 where inr.proc_cd is not null 
;     
;





