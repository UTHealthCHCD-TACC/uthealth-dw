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
select  'truv', 
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
   join dev.truven_dim_uth_claim_id b 
     on b.member_id_src = a.enrolid::text
    and b.claim_id_src = a.msclmid::text
    and b.data_source  = 'truv'
    and b.data_year = a.year
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
select  'truv', 
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
   join dev.truven_dim_uth_claim_id b 
     on b.member_id_src = a.enrolid::text
    and b.claim_id_src = a.msclmid::text
    and b.data_source  = 'truv'
    and b.data_year = a.year
;


