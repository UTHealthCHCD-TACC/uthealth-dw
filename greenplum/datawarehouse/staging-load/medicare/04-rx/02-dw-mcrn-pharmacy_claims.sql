
/* ******************************************************************************************************
 *  load pharmacy_claims for medicare national
 * ******************************************************************************************************
 *  Author  || Date       || Notes
 * ******************************************************************************************************
 *  xiaorui || 10/04/2023 || created script
 * ******************************************************************************************************
 *  xiaorui || 10/09/2023 || hotfix for missing uth_rx_claim_ids
 * */

select 'Medicare National pharmacy claims script started at: ' || current_timestamp as message;

drop table if exists dw_staging.mcrn_pharmacy_claims;

create table dw_staging.mcrn_pharmacy_claims 
(like data_warehouse.pharmacy_claims including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

insert into dw_staging.mcrn_pharmacy_claims(
     data_source, year, uth_rx_claim_id, uth_member_id, fill_date, ndc, 
     days_supply, script_id, refill_count, month_year_id, generic_ind, generic_name, 
     brand_name, provider_npi, total_charge_amount, total_allowed_amount, total_paid_amount, 
     fiscal_year, dispensed_as_written, dose, strength, rx_claim_id_src, member_id_src, table_id_src, load_date, oop, quantity
)
select
    'mcrn' as data_source, 
     extract(year from a.srvc_dt::date) as year, 
    b.uth_rx_claim_id as uth_rx_claim_id, 
    b.uth_member_id as uth_member_id, 
    a.srvc_dt::date as fill_date, 
    a.prod_srvc_id as ndc, 
    a.days_suply_num::int as days_supply, 
    a.rx_srvc_rfrnc_num as script_id, 
    a.fill_num::int as refill_count, 
    get_my_from_date(a.srvc_dt::date) as month_year_id, 
    a.brnd_gnrc_cd as generic_ind, 
    a.gnn as generic_name, 
    a.bn as brand_name, 
    a.srvc_prvdr_id as provider_npi, 
    a.tot_rx_cst_amt::numeric as total_charge_amount, 
    a.tot_rx_cst_amt::numeric as total_allowed_amount, 
    a.cvrd_d_plan_pd_amt::numeric + a.ncvrd_plan_pd_amt::numeric + a.lics_amt::numeric as total_paid_amount, 
    get_fy_from_date(a.srvc_dt::date) as fiscal_year, 
    a.daw_prod_slctn_cd as dispensed_as_written, 
    a.gcdf_desc as dose, 
    a.str as strength, 
    a.pde_id as rx_claim_id_src, 
    a.bene_id as member_id_src, 
    'pde_file' as table_id_src, 
    current_date as load_date, 
    a.ptnt_pay_amt::numeric as oop, 
    a.qty_dspnsd_num::numeric as quantity
from medicare_national.pde_file a
left join data_warehouse.dim_uth_rx_claim_id b
    on a.pde_id = b.rx_claim_id_src and b.data_source = 'mcrn';

vacuum analyze dw_staging.mcrn_pharmacy_claims;

select 'Medicare National pharmacy claims script completed at: ' || current_timestamp as message;

--select * from dw_staging.mcrn_pharmacy_claims;

/**********HOTFIX: some uth_rx_claim_ids fell off the dim table ******
 * 


--check what claims are missing ids
select year, count(*)
from dw_staging.mcrn_pharmacy_claims
where uth_rx_claim_id is null
group by 1 order by 1;

2019	21
2020	13
2021	14460
2022	587
--these are due to bene_ids being null

--look at the sharp spike in 2021
select * from dw_staging.mcrn_pharmacy_claims
where uth_rx_claim_id is null
and year = 2021;


update dw_staging.mcrn_pharmacy_claims a
set uth_rx_claim_id = b.uth_rx_claim_id,
	uth_member_id = b.uth_member_id
from data_warehouse.dim_uth_rx_claim_id b
where a.year = 2021
	and a.uth_rx_claim_id is null
	and b.data_source = 'mcrn'
	and a.rx_claim_id_src = b.rx_claim_id_src;

vacuum analyze dw_staging.mcrn_pharmacy_claims;

select * from dw_staging.mcrn_pharmacy_claims
where uth_rx_claim_id is null;
--21, but these are the ones whose bene_id doesn't exist in the enrollment table

*/






