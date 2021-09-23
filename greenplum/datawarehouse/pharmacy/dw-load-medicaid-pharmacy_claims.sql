


create table dw_staging.medicaid_pharmacy_claims 
with (appendonly=true, orientation=column, compresstype=zlib) as 
select *  
from data_warehouse.pharmacy_claims 
limit 0 
distributed by (uth_member_id);


insert into dw_staging.medicaid_pharmacy_claims (data_source, year, uth_rx_claim_id, uth_member_id,  
                                                 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
                                                 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
                                                 fiscal_year, rx_claim_id_src, member_id_src, table_id_src )
select 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr, get_my_from_date(a.rx_fill_dt) as month_year, 
        a.qty_prescribed, a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid, 
       a.year_fy, pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'chip' 
from medicaid.chip_rx a 
  join data_warehouse.dim_uth_rx_claim_id b  
     on a.pcn = b.member_id_src 
    and pcn || ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
;



insert into dw_staging.medicaid_pharmacy_claims (data_source, year, uth_rx_claim_id, uth_member_id,  
                                                 fill_date, ndc, days_supply, script_id, refill_count, month_year_id, 
                                                 quantity, provider_npi, pharmacy_id, total_charge_amount, total_paid_amount, 
                                                 fiscal_year, rx_claim_id_src, member_id_src, table_id_src )
select 'mdcd', extract(year from a.rx_fill_dt) as yr, b.uth_rx_claim_id, b.uth_member_id, 
       a.rx_fill_dt, a.ndc, a.rx_days_supply, a.rx_nbr, a.refill_nbr, get_my_from_date(a.rx_fill_dt) as month_year, 
        a.qty_prescribed, a.prescriber_npi, a.phmcy_nbr, a.gross_amt_due ,  a.amount_paid, 
       a.year_fy, pcn || ndc || replace(rx_fill_dt::text, '-',''), pcn, 'ffs' 
from medicaid.ffs_rx a
  join data_warehouse.dim_uth_rx_claim_id b  
     on a.pcn = b.member_id_src 
    and pcn || ndc || replace(rx_fill_dt::text, '-','') = b.rx_claim_id_src 
;






