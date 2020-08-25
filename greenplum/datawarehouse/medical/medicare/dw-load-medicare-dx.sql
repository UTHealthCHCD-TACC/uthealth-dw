-- Create target table for DX records

drop table if exists dev.claim_detail_diag_mdcr;

create table dev.claim_detail_diag_mdcr (
id bigserial NOT NULL,
	uth_claim_id numeric,
	uth_member_id int8,
	claim_sequence_number int,
	diag_cd varchar,
	diag_position int,
	poa_src varchar,
	"date" date,
	"year" int2
) 
WITH (appendonly=true, orientation=column)
distributed randomly;


analyze dev.claim_detail_diag_mdcr;

select count(*), year 
from dev.claim_detail_diag_mdcr
group by year 
order by year 


-- Insert Inpatient DX codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 									 									  
select  'mdcr', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare.inpatient_revenue_center_k a 
     join medicare.inpatient_base_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id    
  join data_warehouse.claim_detail d 
    on d.claim_id_src = a.clm_id  
   and d.member_id_src = a.bene_id 
   and d.claim_sequence_number = a.clm_line_num::numeric  
;


-- Outpatient DX codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 												  									  								  
select  'mdcr', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare.outpatient_revenue_center_k a 
     join medicare.outpatient_base_claims_k b
       on b.clm_id = a.clm_id 
      and b.bene_id = a.bene_id    
  join data_warehouse.claim_detail d 
    on d.claim_id_src = a.clm_id  
   and d.member_id_src = a.bene_id 
   and d.claim_sequence_number = a.clm_line_num::numeric  
;


-- Bcarrier DX codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 										  
select  'mdcr', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12])  as dx_pos 
from medicare.bcarrier_claims_k a
  join data_warehouse.claim_detail d 
    on d.claim_id_src = a.clm_id  
   and d.member_id_src = a.bene_id 
   and d.claim_sequence_number = 1
;



-- DME DX codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 											 
select  'mdcr', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12])  as dx_pos 
from medicare.dme_claims_k  a
  join data_warehouse.claim_detail d 
    on d.claim_id_src = a.clm_id  
   and d.member_id_src = a.bene_id 
   and d.claim_sequence_number = 1

   select * from data_warehouse.claim_detail where table_id_src = 'dme_claims_k'

-- HHA DX Codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 											 
select  'mdcr', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare.hha_base_claims_k a
  join data_warehouse.claim_detail d 
    on d.claim_id_src = a.clm_id  
   and d.member_id_src = a.bene_id 
   and d.claim_sequence_number = 1

-- Hospice DX Codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 											 
select  'mdcr', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare.hospice_base_claims_k a
  join data_warehouse.claim_detail d 
    on d.claim_id_src = a.clm_id  
   and d.member_id_src = a.bene_id 
   and d.claim_sequence_number = 1


-- SNF DX Codes
insert into data_warehouse.claim_diag (data_source, year, uth_member_id, uth_claim_id, claim_sequence_number
									  ,date, diag_cd, diag_position) 											 
select  'mdcr', d.year, d.uth_member_id, d.uth_claim_id, d.claim_sequence_number
		,d.from_date_of_service
	    ,unnest(array[icd_dgns_cd1,icd_dgns_cd2,icd_dgns_cd3,icd_dgns_cd4,icd_dgns_cd5,icd_dgns_cd6,icd_dgns_cd7,icd_dgns_cd8,
							  icd_dgns_cd9,icd_dgns_cd10,icd_dgns_cd11,icd_dgns_cd12,icd_dgns_cd13,icd_dgns_cd14,icd_dgns_cd15,icd_dgns_cd16,icd_dgns_cd17,
						      icd_dgns_cd18,icd_dgns_cd19,icd_dgns_cd20,icd_dgns_cd21,icd_dgns_cd22,icd_dgns_cd23,icd_dgns_cd24,icd_dgns_cd25]) as dx				
		,unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25])  as dx_pos 
from medicare.snf_base_claims_k a
  join data_warehouse.claim_detail d 
    on d.claim_id_src = a.clm_id  
   and d.member_id_src = a.bene_id 
   and d.claim_sequence_number = 1


delete from data_warehouse.claim_diag where diag_cd is null;

vacuum analyze data_warehouse.claim_diag;


select count(*), year 
from data_warehouse.claim_diag 
where data_source = 'mdcr'
group by year;
