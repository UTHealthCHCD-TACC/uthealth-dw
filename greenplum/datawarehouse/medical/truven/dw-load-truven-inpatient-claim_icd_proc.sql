
/* ******************************************************************************************************
 *  load claim icd proc for truven
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  jw001  || 1/3/2022  ||  removed year, fiscal_year, icd_type
 * ****************************************************************************************************** 
 *  iperez  || 09/28/2022 || added claim id source and member id source to columns
 * ******************************************************************************************************
 *  iperez  || 09/30/2022 || removed claim id source and member id source to columns
 * ******************************************************************************************************
 * */


create table dw_staging.truven_icd_proc_stage 
with (appendonly=true, orientation=column) as 
select * 
from dw_staging.claim_icd_proc 
limit 0 
distributed by (uth_member_id)
;

delete from dw_staging.claim_icd_proc where data_source = 'truv'
;

-----commercial 
insert into dw_staging.truven_icd_proc_stage ( data_source, 
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
insert into dw_staging.truven_icd_proc_stage ( data_source, 
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


analyze dw_staging.truven_icd_proc_stage

insert into dw_staging.claim_icd_proc 
select distinct * 
from dw_staging.truven_icd_proc_stage 
;

analyze dw_staging.claim_icd_proc;

select data_source, extract(year from from_date_of_service) as yr , count(*) 
from dw_staging.claim_icd_proc 
group by 1,2 order by 1,2 
;