
/* ******************************************************************************************************
 *  load claim header for medicare texas
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  xzhang  || 09/28/2023 || rewrote using Excel, various fixes including mistakes in column mapping
 * ****************************************************************************************************** 
 *  iperez  || 02/06/2026 || modified existing medicare sciprt for medicare enc tables
 * ****************************************************************************************************** 
 * */

select 'mcet claim header script started at ' || current_timestamp as message;

drop table if exists dw_staging.mcet_claim_header;

create table dw_staging.mcet_claim_header 
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
insert into dw_staging.mcet_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcet' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service,
    NULL as total_charge_amount, 
    NULL as total_allowed_amount, 
    NULL as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi as bill_provider, 
    NULL as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.enc_join_key as claim_id_src, 
    a.bene_id as member_id_src, 
    'inpatient' as table_id_src, 
    current_date as load_date,
    NULL as deductible, 
    NULL as coins
from medicare_enc_texas.inpatient_base_enc a
left join data_warehouse.dim_uth_claim_id b
    on a.enc_join_key = b.claim_id_src and b.data_source = 'mcet';


/***********************************
 * HHA
 **********************************/
insert into dw_staging.mcet_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcet' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service,
    NULL as total_charge_amount, 
    NULL as total_allowed_amount, 
    NULL as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi as bill_provider, 
    a.rfrg_physn_npi as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.enc_join_key as claim_id_src, 
    a.bene_id as member_id_src, 
    'hha' as table_id_src, 
    current_date as load_date, 
    NULL as deductible, 
    NULL as coins
from medicare_enc_texas.hha_base_enc a
left join data_warehouse.dim_uth_claim_id b
    on a.enc_join_key = b.claim_id_src and b.data_source = 'mcet';

/***********************************
 * SNF
 **********************************/
insert into dw_staging.mcet_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcet' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service,
    NULL as total_charge_amount, 
    NULL as total_allowed_amount, 
    NULL as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi as bill_provider, 
    NULL as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.enc_join_key as claim_id_src, 
    a.bene_id as member_id_src, 
    'snf' as table_id_src, 
    current_date as load_date, 
    NULL as deductible, 
    NULL as coins
from medicare_enc_texas.snf_base_enc a
left join data_warehouse.dim_uth_claim_id b
    on a.enc_join_key = b.claim_id_src and b.data_source = 'mcet';


/***********************************
 * Outpatient
 **********************************/
insert into dw_staging.mcet_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, other_provider, perf_rn_provider, perf_at_provider, perf_op_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible, coins
)
select
    'mcet' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'F' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    NULL as total_charge_amount, 
    NULL as total_allowed_amount, 
    NULL as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi_num as bill_provider, 
    a.rfrg_physn_npi as ref_provider, 
    a.ot_physn_npi as other_provider, 
    a.rndrng_physn_npi as perf_rn_provider, 
    a.at_physn_npi as perf_at_provider, 
    a.op_physn_npi as perf_op_provider, 
    a.enc_join_key as claim_id_src, 
    a.bene_id as member_id_src, 
    'outpatient' as table_id_src, 
    current_date as load_date, 
    NULL as deductible, 
    NULL as coins
from medicare_enc_texas.outpatient_base_enc a
left join data_warehouse.dim_uth_claim_id b
    on a.enc_join_key = b.claim_id_src and b.data_source = 'mcet';



/***********************************
 * ---------CARRIER/DME------------
 ***********************************/
   
/***********************************
 * Carrier
 **********************************/
insert into dw_staging.mcet_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible
)
select
    'mcet' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'P' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    NULL as total_charge_amount, 
    NULL as total_allowed_amount, 
    NULL as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi as bill_provider, 
    a.rfrg_npi as ref_provider, 
    a.enc_join_key as claim_id_src, 
    a.bene_id as member_id_src, 
    'bcarrier' as table_id_src, 
    current_date as load_date, 
    NULL as deductible
from medicare_enc_texas.carrier_base_enc a
left join data_warehouse.dim_uth_claim_id b
    on a.enc_join_key = b.claim_id_src and b.data_source = 'mcet';
   
/***********************************
 * DME
 **********************************/
insert into dw_staging.mcet_claim_header(
     data_source, year, uth_member_id, uth_claim_id, claim_type, from_date_of_service, 
     to_date_of_service, total_charge_amount, total_allowed_amount, total_paid_amount, fiscal_year, 
     bill_provider, ref_provider, 
     claim_id_src, member_id_src, table_id_src, load_date, deductible
)
select
    'mcet' as data_source, 
     extract(year from a.clm_from_dt::date) as year, 
    b.uth_member_id as uth_member_id, 
    b.uth_claim_id as uth_claim_id, 
    'P' as claim_type, 
    a.clm_from_dt::date as from_date_of_service, 
    a.clm_thru_dt::date as to_date_of_service, 
    NULL as total_charge_amount, 
    NULL as total_allowed_amount, 
    NULL as total_paid_amount, 
    get_fy_from_date(a.clm_from_dt::date) as fiscal_year, 
    a.org_npi as bill_provider, 
    a.rfrg_npi as ref_provider, 
    a.enc_join_key as claim_id_src, 
    a.bene_id as member_id_src, 
    'dme' as table_id_src, 
    current_date as load_date, 
    NULL as deductible
from medicare_enc_texas.dme_base_enc a
left join data_warehouse.dim_uth_claim_id b
    on a.enc_join_key = b.claim_id_src and b.data_source = 'mcet';

  
/***********************************
 * ---------FINALIZE------------
 ***********************************/
vacuum analyze dw_staging.mcet_claim_header;


/***********QA
select "year", count(*) from dw_staging.mcet_claim_header
where uth_claim_id is null group by "year" order by 1;

select "year", count(*) from dw_staging.mcet_claim_header
where uth_member_id is null group by "year" order by 1;
*/

select 'mcet claim header script finished at ' || current_timestamp as message;
