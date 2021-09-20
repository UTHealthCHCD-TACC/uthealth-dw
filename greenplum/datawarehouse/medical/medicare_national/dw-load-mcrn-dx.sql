

-- Insert Inpatient DX codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 									 									  
select  'mcrn', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare_texas.inpatient_revenue_center_k a 
     join medicare_texas.inpatient_base_claims_k b
       on b.bene_id = a.bene_id 
      and b.clm_id = a.clm_id    
  join data_warehouse.claim_detail d 
    on d.member_id_src = a.bene_id 
   and d.claim_id_src = a.clm_id  
   and d.claim_sequence_number = a.clm_line_num::numeric  
;


-- Outpatient DX codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 												  									  								  
select  'mcrn', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare_texas.outpatient_revenue_center_k a 
     join medicare_texas.outpatient_base_claims_k b
       on b.bene_id = a.bene_id   
      and b.clm_id = a.clm_id  
  join data_warehouse.claim_detail d 
    on d.member_id_src = a.bene_id 
   and d.claim_id_src = a.clm_id  
   and d.claim_sequence_number = a.clm_line_num::numeric  
;


-- Bcarrier DX codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 										  
select  'mcrn', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12])  as dx_pos 
from medicare_texas.bcarrier_claims_k a
  join data_warehouse.claim_detail d 
    on d.member_id_src = a.bene_id 
   and d.claim_id_src = a.clm_id  
   and d.claim_sequence_number = 1
;



-- DME DX codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 											 
select  'mcrn', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12])  as dx_pos 
from medicare_texas.dme_claims_k  a
  join data_warehouse.claim_detail d 
    on d.member_id_src = a.bene_id 
   and d.claim_id_src = a.clm_id  
   and d.claim_sequence_number = 1



-- HHA DX Codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 											 
select  'mcrn', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare_texas.hha_base_claims_k a
  join data_warehouse.claim_detail d 
    on d.member_id_src = a.bene_id 
   and d.claim_id_src = a.clm_id  
   and d.claim_sequence_number = 1

   
-- Hospice DX Codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 											 
select  'mcrn', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare_texas.hospice_base_claims_k a
  join data_warehouse.claim_detail d 
    on d.member_id_src = a.bene_id 
   and d.claim_id_src = a.clm_id  
   and d.claim_sequence_number = 1


-- SNF DX Codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 											 
select  'mcrn', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare_texas.snf_base_claims_k a
  join data_warehouse.claim_detail d 
    on d.member_id_src = a.bene_id 
   and d.claim_id_src = a.clm_id  
   and d.claim_sequence_number = 1


delete from data_warehouse.claim_diag where diag_cd is null;

vacuum analyze data_warehouse.claim_diag;


select count(*), data_source , year 
from data_warehouse.claim_diag 
group by data_source , year 
order by data_source , year 
