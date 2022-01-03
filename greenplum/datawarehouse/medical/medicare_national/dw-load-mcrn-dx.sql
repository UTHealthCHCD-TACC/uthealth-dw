
/* ******************************************************************************************************
 *  load claim diag for medicare texas
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 10/04/2021 || add comment block. migrate to dw_staging load 
 * ****************************************************************************************************** 
 *  gmunoz  || 10/25/2021 || adding dev.fiscal_year_func() logic
 * ****************************************************************************************************** 
 *  jwozny  || 1/02/2022  || removed icd type, year, fiscal_year
 * ******************************************************************************************************
 * */


--------------- BEGIN SCRIPT -------



---create copy of data warehouse table in dw_staging 
drop table if exists dw_staging.claim_diag;

create table dw_staging.claim_diag 
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5) as 
select *
from data_warehouse.claim_diag
where data_source not in ('mcrt','mcrn')
distributed by (uth_member_id) 
;

vacuum analyze dw_staging.claim_diag;

delete from dw_staging.claim_diag where data_source = 'mcrn';


-- Outpatient DX codes
insert into dw_staging.claim_diag (data_source, uth_member_id, uth_claim_id, claim_sequence_number,
								   from_date_of_service, diag_cd, diag_position, poa_src) 														  									  								  
select  'mcrn',d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
		,null
from medicare_national.outpatient_revenue_center_k a 
     join medicare_national.outpatient_base_claims_k b
        on a.bene_id = b.bene_id 
       and a.clm_id = b.clm_id 
     join data_warehouse.dim_uth_claim_id c 
       on a.bene_id = c.member_id_src 
      and a.clm_id = c.claim_id_src 
      and c.data_source = 'mcrn'
  join dw_staging.claim_detail d 
    on c.uth_member_id = d.uth_member_id 
   and c.uth_claim_id = d.uth_claim_id 
   and d.claim_sequence_number = a.clm_line_num::numeric  
;
;


-- Bcarrier DX codes
insert into dw_staging.claim_diag (data_source, uth_member_id, uth_claim_id, claim_sequence_number,
								   from_date_of_service, diag_cd, diag_position, poa_src) 														  									  								  
select  'mcrn',d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12])  as dx_pos 
		,null
from medicare_national.bcarrier_claims_k a
     join medicare_national.bcarrier_line_k b 
        on a.bene_id = b.bene_id 
       and a.clm_id = b.clm_id 
     join data_warehouse.dim_uth_claim_id c 
       on a.bene_id = c.member_id_src 
      and a.clm_id = c.claim_id_src 
      and c.data_source = 'mcrn'
  join dw_staging.claim_detail d 
    on c.uth_member_id = d.uth_member_id 
   and c.uth_claim_id = d.uth_claim_id  
   and d.claim_sequence_number = b.line_num::numeric 
;



-- DME DX codes
insert into dw_staging.claim_diag (data_source, uth_member_id, uth_claim_id, claim_sequence_number,
								   from_date_of_service, diag_cd, diag_position, poa_src) 														  									  								  
select  'mcrn',d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12])  as dx_pos 
		,null
from medicare_national.dme_claims_k  a
     join medicare_national.dme_line_k b 
        on a.bene_id = b.bene_id 
       and a.clm_id = b.clm_id 
     join data_warehouse.dim_uth_claim_id c 
       on a.bene_id = c.member_id_src 
      and a.clm_id = c.claim_id_src 
      and c.data_source = 'mcrn'
  join dw_staging.claim_detail d 
    on c.uth_member_id = d.uth_member_id 
   and c.uth_claim_id = d.uth_claim_id  
   and d.claim_sequence_number = b.line_num::numeric 
;

-- Insert Inpatient DX codes
insert into dw_staging.claim_diag (data_source, uth_member_id, uth_claim_id, claim_sequence_number,
								   from_date_of_service, diag_cd, diag_position, poa_src) 									 									  
select  'mcrn', d."year", d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos
		,null
from medicare_national.inpatient_revenue_center_k a 
     join medicare_national.inpatient_base_claims_k b 
        on a.bene_id = b.bene_id 
       and a.clm_id = b.clm_id 
     join data_warehouse.dim_uth_claim_id c 
       on a.bene_id = c.member_id_src 
      and a.clm_id = c.claim_id_src 
      and c.data_source = 'mcrn'
  join dw_staging.claim_detail d 
    on c.uth_member_id = d.uth_member_id 
   and c.uth_claim_id = d.uth_claim_id 
   and d.claim_sequence_number = a.clm_line_num::numeric  
;


-- HHA DX Codes
insert into dw_staging.claim_diag (data_source, uth_member_id, uth_claim_id, claim_sequence_number,
								   from_date_of_service, diag_cd, diag_position, poa_src) 														  									  								  
select  'mcrn',d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
		,null
from medicare_national.hha_revenue_center_k a 
     join medicare_national.hha_base_claims_k b 
         on a.bene_id = b.bene_id 
       and a.clm_id = b.clm_id 
     join data_warehouse.dim_uth_claim_id c 
       on a.bene_id = c.member_id_src 
      and a.clm_id = c.claim_id_src 
      and c.data_source = 'mcrn'
  join dw_staging.claim_detail d 
    on c.uth_member_id = d.uth_member_id 
   and c.uth_claim_id = d.uth_claim_id  
   and d.claim_sequence_number = a.clm_line_num::numeric  
;

   
-- Hospice DX Codes
insert into dw_staging.claim_diag (data_source, uth_member_id, uth_claim_id, claim_sequence_number,
								   from_date_of_service, diag_cd, diag_position, poa_src) 														  									  								  
select  'mcrn',d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
		,null
from medicare_national.hospice_revenue_center_k a 
     join medicare_national.hospice_base_claims_k b 
         on a.bene_id = b.bene_id 
       and a.clm_id = b.clm_id 
     join data_warehouse.dim_uth_claim_id c 
       on a.bene_id = c.member_id_src 
      and a.clm_id = c.claim_id_src 
      and c.data_source = 'mcrn'
  join dw_staging.claim_detail d 
    on c.uth_member_id = d.uth_member_id 
   and c.uth_claim_id = d.uth_claim_id  
   and d.claim_sequence_number = a.clm_line_num::numeric  
;


-- SNF DX Codes
insert into dw_staging.claim_diag (data_source, uth_member_id, uth_claim_id, claim_sequence_number,
								   from_date_of_service, diag_cd, diag_position, poa_src) 														  									  								  
select  'mcrn',d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
		,null		
from medicare_national.snf_revenue_center_k a 
    join medicare_national.snf_base_claims_k b 
       on a.clm_id = b.clm_id 
      and a.bene_id = b.bene_id 
     join data_warehouse.dim_uth_claim_id c 
       on a.bene_id = c.member_id_src 
      and a.clm_id = c.claim_id_src 
      and c.data_source = 'mcrn'
  join dw_staging.claim_detail d 
    on c.uth_member_id = d.uth_member_id 
   and c.uth_claim_id = d.uth_claim_id  
   and d.claim_sequence_number = a.clm_line_num::numeric
;


--cleanup
delete from dw_staging.claim_diag where diag_cd is null;

--finalize
vacuum analyze dw_staging.claim_diag;

---validate
select count(*), data_source , year 
from dw_staging.claim_diag 
group by data_source , year 
order by data_source , year 


---- END SCRIPT 