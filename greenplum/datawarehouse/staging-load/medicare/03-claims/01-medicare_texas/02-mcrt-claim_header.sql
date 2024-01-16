
/* ******************************************************************************************************
 *  load claim header for medicare texas
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  xzhang  || 09/28/2023 || rewrote using Excel, various fixes including mistakes in column mapping
 * ****************************************************************************************************** 
 * */

select 'mcrt claim header script started at ' || current_timestamp as message;

drop table if exists dw_staging.mcrt_claim_header;

create table dw_staging.mcrt_claim_header 
(like data_warehouse.claim_header including defaults) 
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
insert into dw_staging.mcrt_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    a.clm_tot_chrg_amt::numeric as total_charge_amount, 
    a.clm_pmt_amt::numeric + (clm_pass_thru_per_diem_amt::numeric * clm_utlztn_day_cnt::numeric) + nch_prmry_pyr_clm_pd_amt::numeric + nch_ip_tot_ddctn_amt::numeric + clm_uncompd_care_pmt_amt::numeric  + clm_ip_low_vol_pmt_amt::numeric + clm_hrr_adjstmt_pmt_amt::numeric as total_allowed_amount, 
    a.clm_pmt_amt::numeric + (clm_pass_thru_per_diem_amt::numeric * clm_utlztn_day_cnt::numeric) + clm_uncompd_care_pmt_amt::numeric  + clm_ip_low_vol_pmt_amt::numeric + clm_hrr_adjstmt_pmt_amt::numeric as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi_num as bill_provider, 
    NULL as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    'inpatient' as table_id_src, 
    current_date as load_date, 
    a.nch_bene_ip_ddctbl_amt::numeric + nch_bene_blood_ddctbl_lblty_am::numeric as deductible, 
    a.nch_bene_pta_coinsrnc_lblty_am::numeric as coins
from medicare_texas.inpatient_base_claims_k a
left join data_warehouse.dim_uth_claim_id b
    on a.clm_id = b.claim_id_src and b.data_source = 'mcrt';


/***********************************
 * HHA
 **********************************/
insert into dw_staging.mcrt_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    a.clm_tot_chrg_amt::numeric as total_charge_amount, 
    a.clm_pmt_amt::numeric + nch_prmry_pyr_clm_pd_amt::numeric as total_allowed_amount, 
    a.clm_pmt_amt::numeric as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi_num as bill_provider, 
    a.rfr_physn_npi as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    'hha' as table_id_src, 
    current_date as load_date, 
    NULL as deductible, 
    NULL as coins
from medicare_texas.hha_base_claims_k a
left join data_warehouse.dim_uth_claim_id b
    on a.clm_id = b.claim_id_src and b.data_source = 'mcrt';

/***********************************
 * Hospice
 **********************************/
insert into dw_staging.mcrt_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    a.clm_tot_chrg_amt::numeric as total_charge_amount, 
    a.clm_pmt_amt::numeric + nch_prmry_pyr_clm_pd_amt::numeric as total_allowed_amount, 
    a.clm_pmt_amt::numeric as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi_num as bill_provider, 
    a.rfr_physn_npi as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    'hospice' as table_id_src, 
    current_date as load_date, 
    NULL as deductible, 
    NULL as coins
from medicare_texas.hospice_base_claims_k a
left join data_warehouse.dim_uth_claim_id b
    on a.clm_id = b.claim_id_src and b.data_source = 'mcrt';

/***********************************
 * SNF
 **********************************/
insert into dw_staging.mcrt_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    a.clm_tot_chrg_amt::numeric as total_charge_amount, 
    a.clm_pmt_amt::numeric + nch_prmry_pyr_clm_pd_amt::numeric + nch_ip_tot_ddctn_amt::numeric as total_allowed_amount, 
    a.clm_pmt_amt::numeric as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi_num as bill_provider, 
    NULL as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    'snf' as table_id_src, 
    current_date as load_date, 
    a.nch_bene_ip_ddctbl_amt::numeric + nch_bene_blood_ddctbl_lblty_am::numeric as deductible, 
    a.nch_bene_pta_coinsrnc_lblty_am::numeric as coins
from medicare_texas.snf_base_claims_k a
left join data_warehouse.dim_uth_claim_id b
    on a.clm_id = b.claim_id_src and b.data_source = 'mcrt';


/***********************************
 * Outpatient
 **********************************/
insert into dw_staging.mcrt_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    a.clm_tot_chrg_amt::numeric as total_charge_amount, 
    a.clm_pmt_amt::numeric + nch_prmry_pyr_clm_pd_amt::numeric + nch_bene_ptb_coinsrnc_amt::numeric + nch_bene_ptb_ddctbl_amt::numeric + nch_bene_blood_ddctbl_lblty_am::numeric as total_allowed_amount, 
    a.clm_pmt_amt::numeric as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi_num as bill_provider, 
    a.rfr_physn_npi as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    'outpatient' as table_id_src, 
    current_date as load_date, 
    a.nch_bene_ptb_ddctbl_amt::numeric + nch_bene_blood_ddctbl_lblty_am::numeric as deductible, 
    a.nch_bene_ptb_coinsrnc_amt::numeric as coins
from medicare_texas.outpatient_base_claims_k a
left join data_warehouse.dim_uth_claim_id b
    on a.clm_id = b.claim_id_src and b.data_source = 'mcrt';



/***********************************
 * ---------BCARRIER/DME------------
 ***********************************/
   
/***********************************
 * Bcarrier
 **********************************/
insert into dw_staging.mcrt_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'P' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    a.nch_carr_clm_sbmtd_chrg_amt::numeric as total_charge_amount, 
    a.nch_carr_clm_alowd_amt::numeric as total_allowed_amount, 
    a.clm_pmt_amt::numeric as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.cpo_org_npi_num as bill_provider, 
    a.rfr_physn_npi as ref_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    'bcarrier' as table_id_src, 
    current_date as load_date, 
    a.carr_clm_cash_ddctbl_apld_amt::numeric as deductible
from medicare_texas.bcarrier_claims_k a
left join data_warehouse.dim_uth_claim_id b
    on a.clm_id = b.claim_id_src and b.data_source = 'mcrt';
   
/***********************************
 * DME
 **********************************/
insert into dw_staging.mcrt_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible
)
select
    'mcrt' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'P' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    a.nch_carr_clm_sbmtd_chrg_amt::numeric as total_charge_amount, 
    a.nch_carr_clm_alowd_amt::numeric as total_allowed_amount, 
    a.clm_pmt_amt::numeric as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    NULL as bill_provider, 
    a.rfr_physn_npi as ref_provider, 
    a.clm_id as claim_id_src, 
    a.bene_id as member_id_src, 
    'dme' as table_id_src, 
    current_date as load_date, 
    a.carr_clm_cash_ddctbl_apld_amt::numeric as deductible
from medicare_texas.dme_claims_k a
left join data_warehouse.dim_uth_claim_id b
    on a.clm_id = b.claim_id_src and b.data_source = 'mcrt';

/****************************
 * Coinsurance only exists on line-level tables
 * so get numbers from there and sum it up
 */

drop table if exists dw_staging.mcrt_coinsrnc_sum;

create table dw_staging.mcrt_coinsrnc_sum as
select clm_id, bene_id, 'bcarrier' as table_id_src,
 sum(coalesce(line_coinsrnc_amt::numeric, 0)) as coins
from medicare_texas.bcarrier_line_k
group by 1, 2;

insert into dw_staging.mcrt_coinsrnc_sum
select clm_id, bene_id, 'dme' as table_id_src,
 sum(coalesce(line_coinsrnc_amt::numeric, 0)) as coins
from medicare_texas.dme_line_k
group by 1, 2;

analyze dw_staging.mcrt_coinsrnc_sum;
analyze dw_staging.mcrt_claim_header;

update dw_staging.mcrt_claim_header a
set coins = b.coins
from dw_staging.mcrt_coinsrnc_sum b
where a.claim_id_src = b.clm_id and a.member_id_src = b.bene_id
	and a.table_id_src in ('bcarrier', 'dme')
;

drop table if exists dw_staging.mcrt_coinsrnc_sum;
  
/***********************************
 * ---------FINALIZE------------
 ***********************************/
vacuum analyze dw_staging.mcrt_claim_header;

--update the oop (~7 mins)
update dw_staging.mcrt_claim_header
set oop = deductible + coins;
	   	
vacuum analyze dw_staging.mcrt_claim_header;	   	


/***********QA
select "year", count(*) from dw_staging.mcrt_claim_header
where uth_claim_id is null group by "year" order by 1;

select "year", count(*) from dw_staging.mcrt_claim_header
where uth_member_id is null group by "year" order by 1;
*/

select 'mcrt claim header script finished at ' || current_timestamp as message;
