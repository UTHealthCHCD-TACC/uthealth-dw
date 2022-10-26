
/* ******************************************************************************************************
 *  load claim detail for optum zip and optum dod 
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wc001  || 9/29/2021 || script creation - pointed at dev schema for testing
 * ****************************************************************************************************** 
 *  wc002  || 12/9/2021 || modify to do end script
 * ****************************************************************************************************** 
 * */

drop table if exists dw_staging.pharmacy_claims ;

create table dw_staging.pharmacy_claims 
(like data_warehouse.pharmacy_claims including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;

---chip 
insert into dw_staging.pharmacy_claims (
data_source, year, uth_rx_claim_id, uth_member_id,  
 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
 fiscal_year, rx_claim_id_src, member_id_src, table_id_src 
 )
select distinct 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr, get_my_from_date(a.rx_fill_dt) as month_year, 
        a.qty_prescribed, a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid, 
       a.year_fy, pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'chip' 
from medicaid.chip_rx a 
  join data_warehouse.dim_uth_rx_claim_id  b  
     on a.pcn = b.member_id_src 
    and pcn || ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
    and data_source = 'mdcd'
;

analyze dw_staging.pharmacy_claims;

---ffs 
insert into dw_staging.pharmacy_claims (
data_source, year, uth_rx_claim_id, uth_member_id,  
 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
 fiscal_year, rx_claim_id_src, member_id_src, table_id_src )
select distinct 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr, get_my_from_date(a.rx_fill_dt) as month_year, 
        floor(a.rx_quantity::numeric)::int, a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid, 
       a.year_fy, pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'ffs_rx' 
from medicaid.ffs_rx a
  join data_warehouse.dim_uth_rx_claim_id  b  
     on a.pcn = b.member_id_src 
    and pcn || ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
    and data_source = 'mdcd'
;

vacuum analyze dw_staging.pharmacy_claims;

---mco 
insert into dw_staging.pharmacy_claims (
data_source, year, uth_rx_claim_id, uth_member_id,  
 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
 fiscal_year, rx_claim_id_src, member_id_src, table_id_src )
select distinct 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr::int, get_my_from_date(a.rx_fill_dt) as month_year, 
        a.rx_quantity , a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid::float,
       a.year_fy, pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'mco_rx' 
from medicaid.mco_rx a 
   join data_warehouse.dim_uth_rx_claim_id  b 
      on a.pcn = b.member_id_src 
     and pcn || a.ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
     and b.data_source = 'mdcd'
;   

vacuum analyze dw_staging.pharmacy_claims;

---htw
insert into dw_staging.pharmacy_claims (
data_source, year, uth_rx_claim_id, uth_member_id,  
 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
 fiscal_year, rx_claim_id_src, member_id_src, table_id_src )
select distinct 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr, get_my_from_date(a.rx_fill_dt) as month_year, 
        floor(a.rx_quantity::numeric)::int, a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid, 
       dev.fiscal_year_func(a.rx_fill_dt), pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'htw_ffs_rx' 
from medicaid.htw_ffs_rx a
  join data_warehouse.dim_uth_rx_claim_id  b  
     on a.pcn = b.member_id_src 
    and pcn || ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
    and data_source = 'mdcd'
;   

vacuum analyze dw_staging.pharmacy_claims;
grant select on dw_staging.pharmacy_claims to uthealth_analyst;

----finalize 
