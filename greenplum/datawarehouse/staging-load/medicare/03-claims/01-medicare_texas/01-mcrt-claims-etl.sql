/************************************************
 * ETL the medicare claims tables so that life can get better
 * 
 * Note that we're using inner joins on the table pairs (e.g. bcarrier_claims_k / bcarrier_line_k)
 * and it looks like the Medicare tables are so clean that there is no discrepancy
 ************************************************/

/***************************************
 * Initializes ETL table
 **************************************/
drop table if exists dw_staging.mcrt_claims_etl;

create table dw_staging.mcrt_claims_etl (
    --general information
	claim_id_src text,
	member_id_src text,
	--from base claims
	from_date_of_service date,
	to_date_of_service date,
	place_of_service text,
	admit_date date,
	discharge_date date,
	discharge_status bpchar(2),
	drg_cd text,
	bill_type_inst bpchar(1),
	bill_type_class bpchar(1),
	bill_type_freq bpchar(1),
	--from revenue center
	claim_sequence_number int4,
	cpt_hcpcs_cd text,
	procedure_type text,
	proc_mod_1 bpchar(2),
	proc_mod_2 bpchar(2),
	revenue_cd bpchar(4),
	units float8,
	charge_amount numeric,
	allowed_amount numeric,
	paid_amount numeric,
	deductible numeric,
	coins numeric,
	--providers (base claims)
	perf_rn_provider text,
	perf_at_provider text,
	provider_type text,
	bill_provider text,
	--total charges (base claims)
	total_charge_amount numeric,
	total_paid_amount numeric,
	total_allowed_amount numeric,
	--last thing
	table_id_src text
)
distributed by (claim_id_src);

/*********************************
 * Medicare tables set 1:
 * Inpatient, HHA, Hospice, SNF, and Outpatient
 * 		base claims tables summarize the whole claim
 * 		revenue center tables are line items
 *********************************/

/***************************************
 * Inpatient
 **************************************/
insert into dw_staging.mcrt_claims_etl
select extract(year from a.clm_from_dt::date) as year,
	--general information
	a.claim_id as claim_id_src
	a.bene_id as member_id_src
	--from base claims
	a.clm_thru_dt as from_date_of_service
	a.clm_fac_type_cd as to_date_of_service
	a.clm_admsn_dt as place_of_service
	a.nch_bene_dschrg_dt as admit_date
	a.ptnt_dschrg_stus_cd as discharge_date
	a.clm_drg_cd as discharge_status
	a.clm_fac_type_cd as drg_cd
	a.clm_srvc_clsfctn_type_cd as bill_type_inst
	a.clm_freq_cd as bill_type_class
	b.clm_line_num as bill_type_freq
	--from revenue center
	b.hcpcs_cd as claim_sequence_number
	b.hcpcs_1st_mdfr_cd as cpt_hcpcs_cd
	b.hcpcs_2nd_mdfr_cd as procedure_type
	b.rev_cntr as proc_mod_1
	b.rev_cntr_unit_cnt as proc_mod_2
	b.rev_cntr_tot_chrg_amt as revenue_cd
	b.null as units
	b.null as charge_amount
	b.null as allowed_amount
	b.null as paid_amount
	a.rndrng_physn_npi as deductible
	a.at_physn_npi as coins
	--providers (base claims)
	b.org_npi_num as perf_rn_provider
	a.clm_tot_chrg_amt as perf_at_provider
	a.clm_pmt_amt as provider_type
	b.null as bill_provider
	--total charges (base claims)
	--general information as total_charge_amount
	a.claim_id as claim_id_src as total_paid_amount
	a.bene_id as member_id_src as total_allowed_amount
	--last thing
	'inpatient' as table_id_src	
from medicare_texas.inpatient_base_claims_k a
inner join medicare_texas.inpatient_revenue_center_k b 
       on a.bene_id = b.bene_id 
      and a.clm_id = b.clm_id;

/***************************************
 * HHA
 **************************************/
insert into dw_staging.mcrt_claims_etl
select extract(year from a.clm_from_dt::date) as year,
	--general information
	a.claim_id as claim_id_src
	a.bene_id as member_id_src
	--from base claims
	a.clm_thru_dt as from_date_of_service
	a.clm_fac_type_cd as to_date_of_service
	a.clm_admsn_dt as place_of_service
	a.nch_bene_dschrg_dt as admit_date
	a.ptnt_dschrg_stus_cd as discharge_date
	b.null as discharge_status
	a.clm_fac_type_cd as drg_cd
	a.clm_srvc_clsfctn_type_cd as bill_type_inst
	a.clm_freq_cd as bill_type_class
	b.clm_line_num as bill_type_freq
	--from revenue center
	b.hcpcs_cd as claim_sequence_number
	b.hcpcs_1st_mdfr_cd as cpt_hcpcs_cd
	b.hcpcs_2nd_mdfr_cd as procedure_type
	b.rev_cntr as proc_mod_1
	b.rev_cntr_unit_cnt as proc_mod_2
	b.rev_cntr_tot_chrg_amt as revenue_cd
	b.null as units
	b.null as charge_amount
	b.null as allowed_amount
	b.null as paid_amount
	a.rndrng_physn_npi as deductible
	a.at_physn_npi as coins
	--providers (base claims)
	b.org_npi_num as perf_rn_provider
	a.clm_tot_chrg_amt as perf_at_provider
	a.clm_pmt_amt as provider_type
	b.null as bill_provider
	--total charges (base claims)
	--general information as total_charge_amount
	a.claim_id as claim_id_src as total_paid_amount
	a.bene_id as member_id_src as total_allowed_amount
	--last thing
	'hha' as table_id_src	
from medicare_texas.hha_base_claims_k a
inner join medicare_texas.hha_revenue_center_k b 
       on a.bene_id = b.bene_id 
      and a.clm_id = b.clm_id;

/***************************************
 * Hospice
 **************************************/
insert into dw_staging.mcrt_claims_etl
select extract(year from a.clm_from_dt::date) as year,
	--general information
	a.claim_id as claim_id_src
	a.bene_id as member_id_src
	--from base claims
	a.clm_thru_dt as from_date_of_service
	a.clm_fac_type_cd as to_date_of_service
	b.null as place_of_service
	a.nch_bene_dschrg_dt as admit_date
	a.ptnt_dschrg_stus_cd as discharge_date
	b.null as discharge_status
	a.clm_fac_type_cd as drg_cd
	a.clm_srvc_clsfctn_type_cd as bill_type_inst
	a.clm_freq_cd as bill_type_class
	b.clm_line_num as bill_type_freq
	--from revenue center
	b.hcpcs_cd as claim_sequence_number
	b.hcpcs_1st_mdfr_cd as cpt_hcpcs_cd
	b.hcpcs_2nd_mdfr_cd as procedure_type
	b.rev_cntr as proc_mod_1
	b.rev_cntr_unit_cnt as proc_mod_2
	b.rev_cntr_tot_chrg_amt as revenue_cd
	b.null as units
	b.null as charge_amount
	b.null as allowed_amount
	b.null as paid_amount
	a.rndrng_physn_npi as deductible
	a.at_physn_npi as coins
	--providers (base claims)
	b.org_npi_num as perf_rn_provider
	a.clm_tot_chrg_amt as perf_at_provider
	a.clm_pmt_amt as provider_type
	b.null as bill_provider
	--total charges (base claims)
	--general information as total_charge_amount
	a.claim_id as claim_id_src as total_paid_amount
	a.bene_id as member_id_src as total_allowed_amount
	--last thing
	'hospice' as table_id_src
from medicare_texas.hospice_base_claims_k a
inner join medicare_texas.hospice_revenue_center_k b 
       on a.bene_id = b.bene_id 
      and a.clm_id = b.clm_id;

/***************************************
 * SNF
 **************************************/
insert into dw_staging.mcrt_claims_etl
select extract(year from a.clm_from_dt::date) as year,
	--general information
	a.claim_id as claim_id_src
	a.bene_id as member_id_src
	--from base claims
	a.clm_thru_dt as from_date_of_service
	a.clm_fac_type_cd as to_date_of_service
	a.clm_admsn_dt as place_of_service
	a.nch_bene_dschrg_dt as admit_date
	a.ptnt_dschrg_stus_cd as discharge_date
	b.null as discharge_status
	a.clm_fac_type_cd as drg_cd
	a.clm_srvc_clsfctn_type_cd as bill_type_inst
	a.clm_freq_cd as bill_type_class
	b.clm_line_num as bill_type_freq
	--from revenue center
	b.hcpcs_cd as claim_sequence_number
	b.hcpcs_1st_mdfr_cd as cpt_hcpcs_cd
	b.hcpcs_2nd_mdfr_cd as procedure_type
	b.rev_cntr as proc_mod_1
	b.rev_cntr_unit_cnt as proc_mod_2
	b.rev_cntr_tot_chrg_amt as revenue_cd
	b.null as units
	b.null as charge_amount
	b.null as allowed_amount
	b.null as paid_amount
	a.rndrng_physn_npi as deductible
	a.at_physn_npi as coins
	--providers (base claims)
	b.org_npi_num as perf_rn_provider
	a.clm_tot_chrg_amt as perf_at_provider
	a.clm_pmt_amt as provider_type
	b.null as bill_provider
	--total charges (base claims)
	--general information as total_charge_amount
	a.claim_id as claim_id_src as total_paid_amount
	a.bene_id as member_id_src as total_allowed_amount
	--last thing
	'snf' as table_id_src
from medicare_texas.snf_base_claims_k a
inner join medicare_texas.snf_revenue_center_k b 
       on a.bene_id = b.bene_id 
      and a.clm_id = b.clm_id;

/***************************************
 * Outpatient
 **************************************/
insert into dw_staging.mcrt_claims_etl
select extract(year from a.clm_from_dt::date) as year,
	--general information
	--last thing
	'snf' as table_id_src
from medicare_texas.outpatient_base_claims_k a
inner join medicare_texas.outpatient_revenue_center_k b 
       on a.bene_id = b.bene_id 
      and a.clm_id = b.clm_id;

/***************************************
 * B Carrier
 **************************************/
insert into dw_staging.mcrt_claims_etl
select extract(year from a.clm_from_dt::date) as year,
	--general information
	--last thing
	'snf' as table_id_src
from medicare_texas.bcarrier_claims_k a
inner join medicare_texas.bcarrier_line_k b 
       on a.bene_id = b.bene_id 
      and a.clm_id = b.clm_id;
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     

