
/* ******************************************************************************************************
 *  load claim header for medicare texas
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wcc001  || 10/01/2021 || add comment block. migrate to dw_staging load 
 * ****************************************************************************************************** 
 *  gmunoz  || 10/20/2021 || added fiscal year logic function with dev.fiscal_year_func
 * ******************************************************************************************************
 *  jwozny  || 11/08/2021 || added provider variables 
 * ******************************************************************************************************
 *  jwozny  || 12/17/2021 || changed claim_type value to 'F' or 'P' based on which raw dataset is being loaded
 * ******************************************************************************************************
 *  jwozny  || 03/03/2022 || updated payment variables
 * ******************************************************************************************************
 * */


--------------- BEGIN SCRIPT -------

do $$

begin  

--inpatient
insert into dw_staging.claim_header (data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service, 
                                uth_admission_id, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year,
                                bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)							        						     
select 'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, 'F', a.clm_from_dt::date, a.clm_thru_dt::date, 
        null, 
        a.clm_tot_chrg_amt::numeric, 
       	clm_pmt_amt::numeric + clm_pass_thru_per_diem_amt::numeric * clm_utlztn_day_cnt::int + ---- allowed amount jw 3/3/2022
        a.nch_bene_ip_ddctbl_amt::numeric + a.nch_bene_pta_coinsrnc_lblty_am::numeric + nch_bene_blood_ddctbl_lblty_am::numeric + nch_prmry_pyr_clm_pd_amt::numeric as allowed_amt, 
        clm_pmt_amt::numeric + clm_pass_thru_per_diem_amt::numeric * clm_utlztn_day_cnt::int as total_paid_amount,
      	dev.fiscal_year_func(a.clm_thru_dt::date) as fiscal_year,
      	a.org_npi_num as bill_provider, null as ref_provider, a.ot_physn_npi as other_provider, 
      	a.rndrng_physn_npi as perf_rn_provider, a.at_physn_npi as perf_at_provider, a.op_physn_npi as perf_op_provider
from medicare_texas.inpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;

--outpatient
insert into dw_staging.claim_header (data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service, 
                                uth_admission_id, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year,
                                bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)							        						     
select  'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, 'F', a.clm_from_dt::date, a.clm_thru_dt::date, 
        null, a.clm_tot_chrg_amt::numeric, 
        a.clm_pmt_amt::numeric + a.nch_bene_ptb_ddctbl_amt::numeric + a.nch_bene_ptb_coinsrnc_amt::numeric + ---- allowed amount jw 3/3/2022
  			a.nch_bene_blood_ddctbl_lblty_am::numeric + a.nch_prmry_pyr_clm_pd_amt::numeric as allowed_amt,
        a.clm_pmt_amt::numeric, 
      	dev.fiscal_year_func(a.clm_thru_dt::date) as fiscal_year,
      	a.org_npi_num as bill_provider, a.rfr_physn_npi as ref_provider, a.ot_physn_npi as other_provider, 
      	a.rndrng_physn_npi as perf_rn_provider, a.at_physn_npi as perf_at_provider, a.op_physn_npi as perf_op_provider
from medicare_texas.outpatient_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on  c.member_id_src = a.bene_id  
   and c.claim_id_src = a.clm_id
   and c.data_source = b.data_source
;


--bcarrier 
insert into dw_staging.claim_header (data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service, 
                                uth_admission_id, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year,
                                bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)							        						     
select  'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, 'P', a.clm_from_dt::date, a.clm_thru_dt::date,
        null, a.nch_carr_clm_sbmtd_chrg_amt::numeric, a.nch_carr_clm_alowd_amt::numeric, a.clm_pmt_amt::numeric, 
      	dev.fiscal_year_func(a.clm_thru_dt::date) as fiscal_year,
      	a.carr_clm_blg_npi_num as bill_provider, a.rfr_physn_npi as ref_provider, a.cpo_org_npi_num as other_provider, 
      	null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.bcarrier_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.member_id_src = bene_id
   and b.data_source = 'mcrt'
  join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id 
   and c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
;


--dme
insert into dw_staging.claim_header (data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service, 
                                uth_admission_id, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year,
                                bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)			     
select  'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, 'P', a.clm_from_dt::date, a.clm_thru_dt::date,
        null, a.nch_carr_clm_sbmtd_chrg_amt::numeric, a.nch_carr_clm_alowd_amt::numeric, a.clm_pmt_amt::numeric, 
      	dev.fiscal_year_func(a.clm_thru_dt::date) as fiscal_year,
      	null as bill_provider, a.rfr_physn_npi ref_provider, null as other_provider, 
        null as perf_rn_provider, null as perf_at_provider, null as perf_op_provider
from medicare_texas.dme_claims_k a
  join data_warehouse.dim_uth_member_id b 
   on b.member_id_src = bene_id
  and b.data_source = 'mcrt'
  join data_warehouse.dim_uth_claim_id c 
    on c.member_id_src = a.bene_id 
   and c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
;

--hha
insert into dw_staging.claim_header (data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service, 
                                uth_admission_id, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year,
                                bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)							        						     
select  'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, 'F', a.clm_from_dt::date, a.clm_thru_dt::date, 
        null, a.clm_tot_chrg_amt::numeric, a.clm_pmt_amt::numeric + a.nch_prmry_pyr_clm_pd_amt::numeric, a.clm_pmt_amt::numeric, 
      	dev.fiscal_year_func(a.clm_thru_dt::date) as fiscal_year,
      	a.org_npi_num as bill_provider, a.rfr_physn_npi as ref_provider, a.ot_physn_npi as other_provider, 
      	a.rndrng_physn_npi as perf_rn_provider, a.at_physn_npi as perf_at_provider, a.op_physn_npi as perf
from medicare_texas.hha_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;



--hospice
insert into dw_staging.claim_header (data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service, 
                                uth_admission_id, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year,
                                bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)							        						     
select  'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, 'F', a.clm_from_dt::date, a.clm_thru_dt::date, 
        null, a.clm_tot_chrg_amt::numeric, a.clm_pmt_amt::numeric + a.nch_prmry_pyr_clm_pd_amt::numeric, a.clm_pmt_amt::numeric, 
      	dev.fiscal_year_func(a.clm_thru_dt::date) as fiscal_year,
      	 a.org_npi_num as bill_provider, a.rfr_physn_npi as ref_provider, a.ot_physn_npi as other_provider, 
      	a.rndrng_physn_npi as perf_rn_provider, a.at_physn_npi as perf_at_provider, a.op_physn_npi as perf
from medicare_texas.hospice_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;


--snf
insert into dw_staging.claim_header (data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, to_date_of_service, 
                                uth_admission_id, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year,
                                bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider)							        						     
select  'mcrt', extract(year from a.clm_thru_dt::date), c.uth_member_id, c.uth_claim_id, 'F', a.clm_from_dt::date, a.clm_thru_dt::date, 
        null, a.clm_tot_chrg_amt::numeric, 
        a.clm_pmt_amt::numeric + a.nch_bene_ip_ddctbl_amt::numeric + a.nch_bene_pta_coinsrnc_lblty_am::numeric + 
        a.nch_bene_blood_ddctbl_lblty_am::numeric + a.nch_prmry_pyr_clm_pd_amt::numeric as allowed_amt,
        a.clm_pmt_amt::numeric,
      	dev.fiscal_year_func(a.clm_thru_dt::date) as fiscal_year,
      	a.org_npi_num as bill_provider, null as ref_provider, a.ot_physn_npi as other_provider, 
      	a.rndrng_physn_npi as perf_rn_provider, a.at_physn_npi as perf_at_provider, a.op_physn_npi as perf_op_provider
from medicare_texas.snf_base_claims_k a
  join data_warehouse.dim_uth_member_id b 
    on b.data_source = 'mcrt'
   and b.member_id_src = bene_id
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = b.data_source
   and c.claim_id_src = a.clm_id
   and c.member_id_src = a.bene_id 
;

end $$ 

---finalize
analyze dw_staging.claim_header;

--validate
select data_source, year,  count(*)
from dw_staging.claim_header 
group by data_source, year
order by data_source, year;

------------- / END SCRIPT 






