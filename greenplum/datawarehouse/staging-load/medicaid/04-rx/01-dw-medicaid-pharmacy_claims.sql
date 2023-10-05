
/* ******************************************************************************************************
 *  load claim detail for Medicaid
 * ******************************************************************************************************
 *  Author  || Date       || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wc001   || 9/29/2021  || script creation - pointed at dev schema for testing
 * ****************************************************************************************************** 
 *  wc002   || 12/9/2021  || modify to do end script
 * ******************************************************************************************************
 *  xiaorui || 09/12/2023 || added claim_status
 * ****************************************************************************************************** 
 * */

/*ONE-TIME RUN SCRIPT: add claim_status column to tables 
alter table data_warehouse.pharmacy_claims
add column claim_status varchar(20);

vacuum analyze data_warehouse.pharmacy_claims;
*/

/*on next run: need to change data_source = 'mdcd' to
 * data source in ('mdcd', 'mhtw', 'mcpp') for joining
 * to dim tables
 */

drop table if exists dw_staging.mcd_pharmacy_claims;

create table dw_staging.mcd_pharmacy_claims 
(like data_warehouse.pharmacy_claims including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

---chip 
insert into dw_staging.mcd_pharmacy_claims (
data_source, year, uth_rx_claim_id, uth_member_id,  
 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
 fiscal_year, rx_claim_id_src, member_id_src, table_id_src, claim_status
 )
select distinct 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr, get_my_from_date(a.rx_fill_dt) as month_year, 
         floor(a.rx_quantity::numeric)::int, a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid, 
       a.year_fy, pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'chip_rx', a.claim_status
from medicaid.chip_rx a 
  join data_warehouse.dim_uth_rx_claim_id  b  
     on a.pcn = b.member_id_src 
    and pcn || ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
    and data_source = 'mdcd'
;

analyze dw_staging.mcd_pharmacy_claims;

---ffs 
insert into dw_staging.mcd_pharmacy_claims (
data_source, year, uth_rx_claim_id, uth_member_id,  
 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
 fiscal_year, rx_claim_id_src, member_id_src, table_id_src, claim_status )
select distinct 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr, get_my_from_date(a.rx_fill_dt) as month_year, 
        floor(a.rx_quantity::numeric)::int, a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid, 
       a.year_fy, pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'ffs_rx', a.claim_status 
from medicaid.ffs_rx a
  join data_warehouse.dim_uth_rx_claim_id  b  
     on a.pcn = b.member_id_src 
    and pcn || ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
    and data_source = 'mdcd'
;

vacuum analyze dw_staging.mcd_pharmacy_claims;

---mco 
insert into dw_staging.mcd_pharmacy_claims (
data_source, year, uth_rx_claim_id, uth_member_id,  
 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
 fiscal_year, rx_claim_id_src, member_id_src, table_id_src, claim_status )
select distinct 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr::int, get_my_from_date(a.rx_fill_dt) as month_year, 
        a.rx_quantity , a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid::float,
       a.year_fy, pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'mco_rx', a.claim_status 
from medicaid.mco_rx a 
   join data_warehouse.dim_uth_rx_claim_id  b 
      on a.pcn = b.member_id_src 
     and pcn || a.ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
     and b.data_source = 'mdcd'
;   

vacuum analyze dw_staging.mcd_pharmacy_claims;

---htw
insert into dw_staging.mcd_pharmacy_claims (
data_source, year, uth_rx_claim_id, uth_member_id,  
 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
 fiscal_year, rx_claim_id_src, member_id_src, table_id_src, claim_status )
select distinct 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr, get_my_from_date(a.rx_fill_dt) as month_year, 
        floor(a.rx_quantity::numeric)::int, a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid, 
       dev.fiscal_year_func(a.rx_fill_dt), pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'htw_ffs_rx', a.claim_status 
from medicaid.htw_ffs_rx a
  join data_warehouse.dim_uth_rx_claim_id  b  
     on a.pcn = b.member_id_src 
    and pcn || ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
    and data_source = 'mdcd'
;   

vacuum analyze dw_staging.mcd_pharmacy_claims;
grant select on dw_staging.mcd_pharmacy_claims to uthealth_analyst;

----finalize 















