--bcarrier
insert into dw_qa.claim_header (data_source, uth_claim_id, uth_member_id, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src)      
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.bcarrier_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;




--dme
insert into dw_qa.claim_header (data_source, uth_claim_id, uth_member_id, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src)   
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.dme_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;

--hha
insert into dw_qa.claim_header (data_source, uth_claim_id, uth_member_id, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src)   
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.hha_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;

--hospice
insert into dw_qa.claim_header (data_source, uth_claim_id, uth_member_id, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src)   
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.hospice_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;


--inpatient xxxxxxxxxxxxx
insert into dw_qa.claim_header (data_source, uth_claim_id, uth_member_id, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src)   
						        
						        
select 'mdcr', c.uth_claim_id, b.uth_member_id, a.nch_clm_type_cd, NULL, NULL, 
        a.clm_tot_chrg_amt, NULL, a.clm_pmt_amt, a.clm_id, a.bene_id
from medicare.inpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
;

--outpatient
insert into dw_qa.claim_header (data_source, uth_claim_id, uth_member_id, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src)   
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.outpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;

--snf
insert into dw_qa.claim_header (data_source, uth_claim_id, uth_member_id, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src)   
select distinct  'mdcr', clm_id, bene_id, extract(year from clm_from_dt::date), b.uth_member_id
from medicare.snf_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  left join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
   and c.data_year = extract(year from clm_from_dt::date)
where c.uth_claim_id is null 
;