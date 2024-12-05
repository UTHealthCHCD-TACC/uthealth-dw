
/* ******************************************************************************************************
 *  load claim detail for medicare texas
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  xzhang  || 09/28/2023 || rewrote using Excel, various fixes including mistakes in column mapping
 * ****************************************************************************************************** 
 * 
 * Many columns were added - need to fix before running script again
 * */

select 'mcrt claim detail script started at ' || current_timestamp as message;

drop table if exists dw_staging.mcrt_claim_detail;

create table dw_staging.mcrt_claim_detail 
(like data_warehouse.claim_detail including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id);

/***********************************
 * ----------FACILITY--------------
 ***********************************/

/***********************************
 * Inpatient
 **********************************/
insert into dw_staging.mcrt_claim_detail(
     data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, 
     to_date_of_service, month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, 
     discharge_date, discharge_status, cpt_hcpcs_cd, proc_mod_1, proc_mod_2, 
     drg_cd, revenue_cd, charge_amount, allowed_amount, paid_amount, deductible, coins, 
     bill_type_inst, bill_type_class, bill_type_freq, units, fiscal_year, 
     table_id_src, bill_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, load_date, provider_type, bill,
     ndc, ndc_qty, ndc_unit
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    c.uth_member_id as uth_member_id, 
    c.uth_claim_id as uth_claim_id, 
    b.clm_line_num::int as claim_sequence_number, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    get_my_from_date(a.clm_from_dt::date) as month_year_id, 
    a.clm_fac_type_cd as place_of_service, 
    TRUE as network_ind, 
    TRUE as network_paid_ind, 
    a.clm_admsn_dt::date as admit_date, 
    a.nch_bene_dschrg_dt::date as discharge_date, 
    a.ptnt_dschrg_stus_cd as discharge_status, 
    b.hcpcs_cd as cpt_hcpcs_cd, 
    b.hcpcs_1st_mdfr_cd as proc_mod_1, 
    b.hcpcs_2nd_mdfr_cd as proc_mod_2, 
    a.clm_drg_cd as drg_cd, 
    b.rev_cntr as revenue_cd, 
    b.rev_cntr_tot_chrg_amt::numeric as charge_amount, 
    NULL as allowed_amount, 
    NULL as paid_amount, 
    NULL as deductible, 
    NULL as coins, 
    a.clm_fac_type_cd as bill_type_inst, 
    a.clm_srvc_clsfctn_type_cd as bill_type_class, 
    a.clm_freq_cd as bill_type_freq, 
    b.rev_cntr_unit_cnt::float as units, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    'inpatient' as table_id_src, 
    a.org_npi_num as bill_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    current_date as load_date, 
    a.rndrng_physn_spclty_cd as provider_type, 
    a.clm_fac_type_cd || a.clm_srvc_clsfctn_type_cd || a.clm_freq_cd as bill,
    b.rev_cntr_ide_ndc_upc_num as ndc,
    b.rev_cntr_ndc_qty::numeric as ndc_qty,
    b.rev_cntr_ndc_qty_qlfr_cd as ndc_unit
from medicare_texas.inpatient_base_claims_k a
inner join medicare_texas.inpatient_revenue_center_k b
    on a.clm_id = b.clm_id and a.bene_id = b.bene_id
left join data_warehouse.dim_uth_claim_id c
    on a.clm_id = c.claim_id_src and c.data_source = 'mcrt';
			   							   
/***********************************
 * HHA
 **********************************/
insert into dw_staging.mcrt_claim_detail(
     data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, 
     to_date_of_service, month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, 
     discharge_date, discharge_status, cpt_hcpcs_cd, proc_mod_1, proc_mod_2, 
     drg_cd, revenue_cd, charge_amount, allowed_amount, paid_amount, deductible, coins, 
     bill_type_inst, bill_type_class, bill_type_freq, units, fiscal_year, 
     table_id_src, bill_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, load_date, provider_type, bill,
     ndc, ndc_qty, ndc_unit
)						   							   
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    c.uth_member_id as uth_member_id, 
    c.uth_claim_id as uth_claim_id, 
    b.clm_line_num::int as claim_sequence_number, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    get_my_from_date(a.clm_from_dt::date) as month_year_id, 
    a.clm_fac_type_cd as place_of_service, 
    TRUE as network_ind, 
    TRUE as network_paid_ind, 
    a.clm_admsn_dt::date as admit_date, 
    a.nch_bene_dschrg_dt::date as discharge_date, 
    a.ptnt_dschrg_stus_cd as discharge_status, 
    b.hcpcs_cd as cpt_hcpcs_cd, 
    b.hcpcs_1st_mdfr_cd as proc_mod_1, 
    b.hcpcs_2nd_mdfr_cd as proc_mod_2, 
    NULL as drg_cd, 
    b.rev_cntr as revenue_cd, 
    b.rev_cntr_tot_chrg_amt::numeric as charge_amount, 
    NULL as allowed_amount, 
    b.rev_cntr_prvdr_pmt_amt::numeric as paid_amount, 
    NULL as deductible, 
    NULL as coins, 
    a.clm_fac_type_cd as bill_type_inst, 
    a.clm_srvc_clsfctn_type_cd as bill_type_class, 
    a.clm_freq_cd as bill_type_freq, 
    b.rev_cntr_unit_cnt::float as units, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    'hha' as table_id_src, 
    a.org_npi_num as bill_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    current_date as load_date, 
    a.rndrng_physn_spclty_cd as provider_type, 
    a.clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
    b.rev_cntr_ide_ndc_upc_num as ndc,
    b.rev_cntr_ndc_qty::numeric as ndc_qty,
    b.rev_cntr_ndc_qty_qlfr_cd as ndc_unit
from medicare_texas.hha_base_claims_k a
inner join medicare_texas.hha_revenue_center_k b
    on a.clm_id = b.clm_id and a.bene_id = b.bene_id
left join data_warehouse.dim_uth_claim_id c
    on a.clm_id = c.claim_id_src and c.data_source = 'mcrt';

/***********************************
 * Hospice
 **********************************/
insert into dw_staging.mcrt_claim_detail(
     data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, 
     to_date_of_service, month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, 
     discharge_date, discharge_status, cpt_hcpcs_cd, proc_mod_1, proc_mod_2, 
     drg_cd, revenue_cd, charge_amount, allowed_amount, paid_amount, deductible, coins, 
     bill_type_inst, bill_type_class, bill_type_freq, units, fiscal_year, 
     table_id_src, bill_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, load_date, provider_type, bill,
     ndc, ndc_qty, ndc_unit
)						   							   
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    c.uth_member_id as uth_member_id, 
    c.uth_claim_id as uth_claim_id, 
    b.clm_line_num::int as claim_sequence_number, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    get_my_from_date(a.clm_from_dt::date) as month_year_id, 
    a.clm_fac_type_cd as place_of_service, 
    TRUE as network_ind, 
    TRUE as network_paid_ind, 
    NULL as admit_date, 
    a.nch_bene_dschrg_dt::date as discharge_date, 
    a.ptnt_dschrg_stus_cd as discharge_status, 
    b.hcpcs_cd as cpt_hcpcs_cd, 
    b.hcpcs_1st_mdfr_cd as proc_mod_1, 
    b.hcpcs_2nd_mdfr_cd as proc_mod_2, 
    NULL as drg_cd, 
    b.rev_cntr as revenue_cd, 
    b.rev_cntr_tot_chrg_amt::numeric as charge_amount, 
    NULL as allowed_amount, 
    b.rev_cntr_prvdr_pmt_amt::numeric + b.rev_cntr_bene_pmt_amt::numeric as paid_amount, 
    NULL as deductible, 
    NULL as coins, 
    a.clm_fac_type_cd as bill_type_inst, 
    a.clm_srvc_clsfctn_type_cd as bill_type_class, 
    a.clm_freq_cd as bill_type_freq, 
    b.rev_cntr_unit_cnt::float as units, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    'hospice' as table_id_src, 
    a.org_npi_num as bill_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    current_date as load_date, 
    a.rndrng_physn_spclty_cd as provider_type, 
    a.clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
    b.rev_cntr_ide_ndc_upc_num as ndc,
    b.rev_cntr_ndc_qty::numeric as ndc_qty,
    b.rev_cntr_ndc_qty_qlfr_cd as ndc_unit
from medicare_texas.hospice_base_claims_k a
inner join medicare_texas.hospice_revenue_center_k b
    on a.clm_id = b.clm_id and a.bene_id = b.bene_id
left join data_warehouse.dim_uth_claim_id c
    on a.clm_id = c.claim_id_src and c.data_source = 'mcrt';



/***********************************
 * SNF
 **********************************/
insert into dw_staging.mcrt_claim_detail(
     data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, 
     to_date_of_service, month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, 
     discharge_date, discharge_status, cpt_hcpcs_cd, proc_mod_1, proc_mod_2, 
     drg_cd, revenue_cd, charge_amount, allowed_amount, paid_amount, deductible, coins, 
     bill_type_inst, bill_type_class, bill_type_freq, units, fiscal_year, 
     table_id_src, bill_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, load_date, provider_type, bill,
     ndc, ndc_qty, ndc_unit
)						   							   
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    c.uth_member_id as uth_member_id, 
    c.uth_claim_id as uth_claim_id, 
    b.clm_line_num::int as claim_sequence_number, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    get_my_from_date(a.clm_from_dt::date) as month_year_id, 
    a.clm_fac_type_cd as place_of_service, 
    TRUE as network_ind, 
    TRUE as network_paid_ind, 
    a.clm_admsn_dt::date as admit_date, 
    a.nch_bene_dschrg_dt::date as discharge_date, 
    a.ptnt_dschrg_stus_cd as discharge_status, 
    b.hcpcs_cd as cpt_hcpcs_cd, 
    b.hcpcs_1st_mdfr_cd as proc_mod_1, 
    b.hcpcs_2nd_mdfr_cd as proc_mod_2, 
    NULL as drg_cd, 
    b.rev_cntr as revenue_cd, 
    b.rev_cntr_tot_chrg_amt::numeric as charge_amount, 
    NULL as allowed_amount, 
    NULL as paid_amount, 
    NULL as deductible, 
    NULL as coins, 
    a.clm_fac_type_cd as bill_type_inst, 
    a.clm_srvc_clsfctn_type_cd as bill_type_class, 
    a.clm_freq_cd as bill_type_freq, 
    b.rev_cntr_unit_cnt::float as units, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    'snf' as table_id_src, 
    a.org_npi_num as bill_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    current_date as load_date, 
    a.rndrng_physn_spclty_cd as provider_type, 
    a.clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
    b.rev_cntr_ide_ndc_upc_num as ndc,
    b.rev_cntr_ndc_qty::numeric as ndc_qty,
    b.rev_cntr_ndc_qty_qlfr_cd as ndc_unit
from medicare_texas.snf_base_claims_k a
inner join medicare_texas.snf_revenue_center_k b
    on a.clm_id = b.clm_id and a.bene_id = b.bene_id
left join data_warehouse.dim_uth_claim_id c
    on a.clm_id = c.claim_id_src and c.data_source = 'mcrt';

   
/***********************************
 * Outpatient
 **********************************/
insert into dw_staging.mcrt_claim_detail(
     data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, 
     to_date_of_service, month_year_id, place_of_service, network_ind, network_paid_ind, admit_date, 
     discharge_date, discharge_status, cpt_hcpcs_cd, proc_mod_1, proc_mod_2, 
     drg_cd, revenue_cd, charge_amount, allowed_amount, paid_amount, deductible, coins, 
     bill_type_inst, bill_type_class, bill_type_freq, units, fiscal_year, 
     table_id_src, bill_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, load_date, provider_type, bill,
     ndc, ndc_qty, ndc_unit
)						   							   
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    c.uth_member_id as uth_member_id, 
    c.uth_claim_id as uth_claim_id, 
    b.clm_line_num::int as claim_sequence_number, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    get_my_from_date(a.clm_from_dt::date) as month_year_id, 
    a.clm_fac_type_cd as place_of_service, 
    TRUE as network_ind, 
    TRUE as network_paid_ind, 
    NULL as admit_date, 
    NULL as discharge_date, 
    NULL as discharge_status, 
    b.hcpcs_cd as cpt_hcpcs_cd, 
    b.hcpcs_1st_mdfr_cd as proc_mod_1, 
    b.hcpcs_2nd_mdfr_cd as proc_mod_2, 
    NULL as drg_cd, 
    b.rev_cntr as revenue_cd, 
    b.rev_cntr_tot_chrg_amt::numeric as charge_amount, 
    b.rev_cntr_prvdr_pmt_amt::numeric + b.rev_cntr_bene_pmt_amt::numeric + b.rev_cntr_ptnt_rspnsblty_pmt::numeric as allowed_amount, 
    b.rev_cntr_prvdr_pmt_amt::numeric + b.rev_cntr_bene_pmt_amt::numeric as paid_amount, 
    b.rev_cntr_blood_ddctbl_amt::numeric + b.rev_cntr_cash_ddctbl_amt::numeric as deductible, 
    b.rev_cntr_coinsrnc_wge_adjstd_c::numeric as coins, 
    a.clm_fac_type_cd as bill_type_inst, 
    a.clm_srvc_clsfctn_type_cd as bill_type_class, 
    a.clm_freq_cd as bill_type_freq, 
    b.rev_cntr_unit_cnt::float as units, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    'outpatient' as table_id_src, 
    a.org_npi_num as bill_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    current_date as load_date, 
    a.rndrng_physn_spclty_cd as provider_type, 
    a.clm_fac_type_cd || clm_srvc_clsfctn_type_cd || clm_freq_cd as bill,
    b.rev_cntr_ide_ndc_upc_num as ndc,
    b.rev_cntr_ndc_qty::numeric as ndc_qty,
    b.rev_cntr_ndc_qty_qlfr_cd as ndc_unit
from medicare_texas.outpatient_base_claims_k a
inner join medicare_texas.outpatient_revenue_center_k b
    on a.clm_id = b.clm_id and a.bene_id = b.bene_id
left join data_warehouse.dim_uth_claim_id c
    on a.clm_id = c.claim_id_src and c.data_source = 'mcrt';

   
/***********************************
 * ---------BCARRIER/DME------------
 ***********************************/
   
/***********************************
 * Bcarrier
 **********************************/
insert into dw_staging.mcrt_claim_detail(
     data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, 
     to_date_of_service, month_year_id, place_of_service, network_ind, network_paid_ind, 
     cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, 
     charge_amount, allowed_amount, paid_amount, deductible, coins, 
     units, fiscal_year, table_id_src, bill_provider, 
     claim_id_src, member_id_src, load_date, provider_type
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    c.uth_member_id as uth_member_id, 
    c.uth_claim_id as uth_claim_id, 
    b.line_num::int as claim_sequence_number, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    get_my_from_date(a.clm_from_dt::date) as month_year_id, 
    b.line_place_of_srvc_cd as place_of_service, 
    TRUE as network_ind, 
    TRUE as network_paid_ind, 
    b.hcpcs_cd as cpt_hcpcs_cd, 
    b.hcpcs_cd as procedure_type, 
    b.hcpcs_1st_mdfr_cd as proc_mod_1, 
    b.hcpcs_2nd_mdfr_cd as proc_mod_2, 
    b.line_sbmtd_chrg_amt::numeric as charge_amount, 
    b.line_alowd_chrg_amt::numeric as allowed_amount, 
    b.line_nch_pmt_amt::numeric as paid_amount, 
    b.line_bene_ptb_ddctbl_amt::numeric as deductible, 
    b.line_coinsrnc_amt::numeric as coins, 
    b.line_srvc_cnt::float as units, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    'bcarrier' as table_id_src, 
    b.org_npi_num as bill_provider, 
    b.clm_id as claim_id_src, 
    b.bene_id as member_id_src, 
    current_date as load_date, 
    b.prvdr_spclty as provider_type
from medicare_texas.bcarrier_claims_k a
inner join medicare_texas.bcarrier_line_k b
    on a.clm_id = b.clm_id and a.bene_id = b.bene_id
left join data_warehouse.dim_uth_claim_id c
    on a.clm_id = c.claim_id_src and c.data_source = 'mcrt';
   
   
 /***********************************
 * DME
 **********************************/
insert into dw_staging.mcrt_claim_detail(
     data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, 
     to_date_of_service, month_year_id, place_of_service, network_ind, network_paid_ind, 
     cpt_hcpcs_cd, procedure_type, proc_mod_1, proc_mod_2, 
     charge_amount, allowed_amount, paid_amount, deductible, coins, 
     units, fiscal_year, table_id_src, bill_provider, 
     claim_id_src, member_id_src, load_date, provider_type,
     ndc
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    c.uth_member_id as uth_member_id, 
    c.uth_claim_id as uth_claim_id, 
    b.line_num::int as claim_sequence_number, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    get_my_from_date(a.clm_from_dt::date) as month_year_id, 
    b.line_place_of_srvc_cd as place_of_service, 
    TRUE as network_ind, 
    TRUE as network_paid_ind, 
    b.hcpcs_cd as cpt_hcpcs_cd, 
    b.hcpcs_cd as procedure_type, 
    b.hcpcs_1st_mdfr_cd as proc_mod_1, 
    b.hcpcs_2nd_mdfr_cd as proc_mod_2, 
    b.line_sbmtd_chrg_amt::numeric as charge_amount, 
    b.line_alowd_chrg_amt::numeric as allowed_amount, 
    b.line_nch_pmt_amt::numeric as paid_amount, 
    b.line_bene_ptb_ddctbl_amt::numeric as deductible, 
    b.line_coinsrnc_amt::numeric as coins, 
    b.dmerc_line_mtus_cnt::float as units, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    'dme' as table_id_src, 
    b.prvdr_npi as bill_provider, 
    b.clm_id as claim_id_src, 
    b.bene_id as member_id_src, 
    current_date as load_date, 
    NULL as provider_type,
    b.line_ndc_cd as ndc
from medicare_texas.dme_claims_k a
inner join medicare_texas.dme_line_k b
    on a.clm_id = b.clm_id and a.bene_id = b.bene_id
left join data_warehouse.dim_uth_claim_id c
    on a.clm_id = c.claim_id_src and c.data_source = 'mcrt';


/***********************************
 * ---------FINALIZE------------
 ***********************************/
analyze dw_staging.mcrt_claim_detail;

--update the procedure type (~7 mins)
update dw_staging.mcrt_claim_detail
set procedure_type = 
	case when substring(cpt_hcpcs_cd,1,1) ~ '[0-9]' then 'CPT'
	   	 when substring(cpt_hcpcs_cd,1,1) ~ '[a-zA-Z]' then 'HCPCS'
	   	 else null end;
	   	
vacuum analyze dw_staging.mcrt_claim_detail;	   	


/***********QA
select "year", count(*) from dw_staging.mcrt_claim_detail
where uth_claim_id is null group by "year" order by 1;

select "year", count(*) from dw_staging.mcrt_claim_detail
where uth_member_id is null group by "year" order by 1;
*/

select 'mcrt claim detail script finished at ' || current_timestamp as message;








