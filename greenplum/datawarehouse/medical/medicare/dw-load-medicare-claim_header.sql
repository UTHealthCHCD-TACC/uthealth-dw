
delete from data_warehouse.claim_header where data_source = 'mdcr'

vacuum analyze data_warehouse.claim_header 

--inpatient
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)					        
select 'mdcr', c.data_year,c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, a.clm_fac_type_cd, NULL, NULL, 
        a.clm_tot_chrg_amt::numeric, NULL, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'inpatient_base_claims_k'
from medicare.inpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;

--outpatient
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  
select  'mdcr', c.data_year, c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, a.clm_fac_type_cd, null, null, 
        a.clm_tot_chrg_amt::numeric, null, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'outpatient_base_claims_k'
from medicare.outpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;



--bcarrier 
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  
select  'mdcr', c.data_year,c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, null, 
        a.nch_carr_clm_sbmtd_chrg_amt::numeric, a.nch_carr_clm_alowd_amt::numeric, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'bcarrier_claims_k'
from medicare.bcarrier_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


--dme
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  
select  'mdcr', c.data_year, c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, null, 
        a.nch_carr_clm_sbmtd_chrg_amt::numeric, a.nch_carr_clm_alowd_amt::numeric, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'dme_claims_k'
from medicare.dme_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;

--hha
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  						        						        
select  'mdcr', c.data_year,c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, a.clm_fac_type_cd, null, null, 
        a.clm_tot_chrg_amt::numeric,null, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'hha_base_claims_k'
from medicare.hha_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;



--hospice
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  
select  'mdcr', c.data_year, c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, null, 
        a.clm_tot_chrg_amt::numeric,null, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'hospice_base_claims_k'
from medicare.hospice_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


--snf
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, place_of_service, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src)  
select  'mdcr', c.data_year, c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, null, 
        a.clm_tot_chrg_amt::numeric,null, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'snf_base_claims_k'
from medicare.snf_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mdcr'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


vacuum analyze data_warehouse.claim_header;



select count(*), year, table_id_src  from data_warehouse.claim_header where data_source = 'mdcr'
group by year, table_id_src 
order by year, table_id_src ;








