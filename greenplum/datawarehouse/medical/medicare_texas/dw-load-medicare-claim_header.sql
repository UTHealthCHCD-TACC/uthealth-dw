alter table data_warehouse.claim_header add column to_date_of_service date;

select distinct data_source from data_warehouse.claim_header 

vacuum analyze data_warehouse.claim_header 

--inpatient
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type,  uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year )					        
select 'mcrt', c.data_year,c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, NULL, NULL, 
        a.clm_tot_chrg_amt::numeric, NULL, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'inpatient_base_claims_k', a.year::int2
from medicare_texas.inpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_national.inpatient_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrn'
  ;
   
--outpatient
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year )  
select  'mcrt', c.data_year, c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, 
        a.clm_tot_chrg_amt::numeric, null, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'outpatient_base_claims_k', a.year::int2
from medicare_texas.outpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_texas.outpatient_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrt'
  ;

 
update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_national.outpatient_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrn'
  ; 
 
 

--bcarrier 
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year )  
select  'mcrt', c.data_year,c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, 
        a.nch_carr_clm_sbmtd_chrg_amt::numeric, a.nch_carr_clm_alowd_amt::numeric, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'bcarrier_claims_k', a.year::int2
from medicare_texas.bcarrier_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;

update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from  medicare_texas.bcarrier_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrt'
  ;

 
update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_national.bcarrier_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrn'
  ; 
 

--dme
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year )  
select  'mcrt', c.data_year, c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, 
        a.nch_carr_clm_sbmtd_chrg_amt::numeric, a.nch_carr_clm_alowd_amt::numeric, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'dme_claims_k', a.year::int2
from medicare_texas.dme_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;

update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from  medicare_texas.dme_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrt'
  ;

 
 update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_national.dme_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrn'
  ; 
 
 
--hha
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year )  						        						        
select  'mcrt', c.data_year,c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd,  null, null, 
        a.clm_tot_chrg_amt::numeric,null, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'hha_base_claims_k', a.year::int2
from medicare_texas.hha_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from  medicare_texas.hha_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrt'
  ;

 update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_national.hha_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrn'
  ; 


--hospice
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year )  
select  'mcrt', c.data_year, c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, 
        a.clm_tot_chrg_amt::numeric,null, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'hospice_base_claims_k', a.year::int2
from medicare_texas.hospice_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_texas.hospice_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrt'
;


 update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_national.hospice_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrn'
  ; 


--snf
insert into data_warehouse.claim_header (data_source, year, uth_claim_id, uth_member_id, from_date_of_service, claim_type, uth_admission_id, admission_id_src,
						        total_charge_amount, total_allowed_amount, total_paid_amount, claim_id_src, member_id_src, table_id_src, data_year )  
select  'mcrt', c.data_year, c.uth_claim_id, b.uth_member_id, a.clm_from_dt::date, a.nch_clm_type_cd, null, null, 
        a.clm_tot_chrg_amt::numeric,null, a.clm_pmt_amt::numeric, a.clm_id, a.bene_id, 'snf_base_claims_k', a.year::int2
from medicare_texas.snf_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_texas.snf_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrt'
  ;

 update data_warehouse.claim_header b set to_date_of_service = a.clm_thru_dt::date
from medicare_national.snf_base_claims_k a
where a.clm_id = b.claim_id_src 
  and a.bene_id = b.member_id_src 
  and b.data_source = 'mcrn'
  ;  
 

--cleanup
vacuum analyze data_warehouse.claim_header;


select * from data_warehouse.claim_header ch where data_source = 'mcrt'; --40s


--validate
select count(*), year, table_id_src  
from data_warehouse.claim_header where data_source = 'mcrt'
group by year, table_id_src 
order by table_id_src , year
;



select count(*), claim_type, data_source 
from data_warehouse.claim_header
group by claim_type , data_source 
order by data_source 


update data_warehouse.claim_header set claim_type = 'P' 
where data_source in ('mcrt','mcrn') and table_id_src in ('dme_claims_k','bcarrier_claims_k')
;

update data_warehouse.claim_header set claim_type = 'F' 
where data_source in ('mcrt','mcrn') and table_id_src not in ('dme_claims_k','bcarrier_claims_k')
;

vacuum analyze data_warehouse.claim_header;


select * from data_warehouse.claim_detail cd where data_source = 'truv' and procedure_cd = 'A0425'

select * from data_warehouse.claim_detail cd where data_source = 'optz' and procedure_cd = 'A0422'

select * from data_warehouse.claim_detail cd where data_source = 'mcrt' and procedure_cd = 'A0422'
