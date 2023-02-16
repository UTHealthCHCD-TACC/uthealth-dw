
/* ******************************************************************************************************
 *  load claim icd proc for medicare national
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 10/05/2021 || add comment block. migrate to dw_staging load 
 * ****************************************************************************************************** 
 *  gmunoz  || 10/25/2021 || adding dev.fiscal_year_func() logic
 * ****************************************************************************************************** 
 * */


--------------- BEGIN SCRIPT -------

-- Insert Inpatient proc codes
insert into dw_staging.claim_icd_proc( data_source,  uth_member_id, uth_claim_id, claim_sequence_number, 
                                       from_date_of_service, 
                                       proc_cd, 
                                       proc_position,
                                       icd_version 
                                      )
select 'mcrn',  b.uth_member_id, b.uth_claim_id, 1 as seq, 
		clm_from_dt::date as clm_date,
		unnest(array[icd_prcdr_cd1,icd_prcdr_cd2,icd_prcdr_cd3,icd_prcdr_cd4,icd_prcdr_cd5,icd_prcdr_cd6,icd_prcdr_cd7,icd_prcdr_cd8,
					 icd_prcdr_cd9,icd_prcdr_cd10,icd_prcdr_cd11,icd_prcdr_cd12,icd_prcdr_cd13,icd_prcdr_cd14,icd_prcdr_cd15,icd_prcdr_cd16,icd_prcdr_cd17,
					 icd_prcdr_cd18,icd_prcdr_cd19,icd_prcdr_cd20,icd_prcdr_cd21,icd_prcdr_cd22,icd_prcdr_cd23,icd_prcdr_cd24,icd_prcdr_cd25]) 
		as proc,
		unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as proc_position,
		null
	from medicare_national.inpatient_base_claims_k a
	  join data_warehouse.dim_uth_claim_id b  
	    on b.member_id_src = a.bene_id 
	   and b.claim_id_src = a.clm_id 
	   and b.data_source = 'mcrn' 
;



-- Outpatient Proc codes
insert into dw_staging.claim_icd_proc( data_source,  uth_member_id, uth_claim_id, claim_sequence_number, 
                                       from_date_of_service, 
                                       proc_cd, 
                                       proc_position , 
                                       icd_version 
                                      )
select 'mcrn',  b.uth_member_id, b.uth_claim_id, 1 as seq, 
		clm_from_dt::date as clm_date,
		unnest(array[icd_prcdr_cd1,icd_prcdr_cd2,icd_prcdr_cd3,icd_prcdr_cd4,icd_prcdr_cd5,icd_prcdr_cd6,icd_prcdr_cd7,icd_prcdr_cd8,
					 icd_prcdr_cd9,icd_prcdr_cd10,icd_prcdr_cd11,icd_prcdr_cd12,icd_prcdr_cd13,icd_prcdr_cd14,icd_prcdr_cd15,icd_prcdr_cd16,icd_prcdr_cd17,
					 icd_prcdr_cd18,icd_prcdr_cd19,icd_prcdr_cd20,icd_prcdr_cd21,icd_prcdr_cd22,icd_prcdr_cd23,icd_prcdr_cd24,icd_prcdr_cd25]) 
		as proc,
		unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as proc_position,
		null
	from medicare_national.outpatient_base_claims_k a
	  join data_warehouse.dim_uth_claim_id b  
	    on b.member_id_src = a.bene_id 
	   and b.claim_id_src = a.clm_id 
	   and b.data_source = 'mcrn' 
;

-- SNF Proc codes
insert into dw_staging.claim_icd_proc( data_source,  uth_member_id, uth_claim_id, claim_sequence_number, 
                                       from_date_of_service, 
                                       proc_cd, 
                                       proc_position,
                                       icd_version 
                                      )
select 'mcrn', b.uth_member_id, b.uth_claim_id, 1 as seq, 
		clm_from_dt::date as clm_date,
		unnest(array[icd_prcdr_cd1,icd_prcdr_cd2,icd_prcdr_cd3,icd_prcdr_cd4,icd_prcdr_cd5,icd_prcdr_cd6,icd_prcdr_cd7,icd_prcdr_cd8,
					 icd_prcdr_cd9,icd_prcdr_cd10,icd_prcdr_cd11,icd_prcdr_cd12,icd_prcdr_cd13,icd_prcdr_cd14,icd_prcdr_cd15,icd_prcdr_cd16,icd_prcdr_cd17,
					 icd_prcdr_cd18,icd_prcdr_cd19,icd_prcdr_cd20,icd_prcdr_cd21,icd_prcdr_cd22,icd_prcdr_cd23,icd_prcdr_cd24,icd_prcdr_cd25]) 
		as proc,
		unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25]) as proc_position,
		null
	from medicare_national.snf_base_claims_k a
	  join data_warehouse.dim_uth_claim_id b  
	    on b.member_id_src = a.bene_id 
	   and b.claim_id_src = a.clm_id 
	   and b.data_source = 'mcrn' 
;

---clean up 
delete from dw_staging.claim_icd_proc where proc_cd is null; 


---finalize
vacuum analyze dw_staging.claim_icd_proc;



select data_source, count(*) 
from dw_staging.claim_icd_proc 
group by data_source 
order by data_source  
;

------- END SCRIPT 