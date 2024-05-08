/*************************************************************************************************************
 * Script Purpose | Prepares IQVIA data to be inserted into the data_warehouse.pharmacy_claims table.
 * _______________| 
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 1/19/23  || Sharrah   || Script created.
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           || 
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA pharmacy claims script started at: ' || current_timestamp as message;


--=== Create empty pharmacy_claims table for IQVIA: ===--

-- Drop and create table:
drop table if exists dw_staging.iqva_pharmacy_claims;

create table dw_staging.iqva_pharmacy_claims 
(like data_warehouse.pharmacy_claims including defaults) 
with (
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
) distributed by (uth_member_id);

-- Vacuum analyze:
vacuum analyze dw_staging.iqva_pharmacy_claims;



--=== Insert into dw_staging.iqva_pharmacy_claims from staging_clean.iqva_rx_etl: ===--

-- Timestamp:
select 'Inserting data into dw_staging.iqva_pharmacy_claims from staging_clean.iqva_rx_etl started at: ' || current_timestamp as message;

insert into dw_staging.iqva_pharmacy_claims  (
		data_source,
		year,
		uth_rx_claim_id,
		uth_member_id,
		fill_date,
		ndc,
		days_supply,
		month_year_id,
		generic_name,
		brand_name,
		provider_npi,
		total_allowed_amount, 
		total_paid_amount,
		deductible, 
		copay, 
		coins, 
		cob,
		fiscal_year,
		therapeutic_class,
		dispensed_as_written,
		dose, 
		strength, 
		formulary_ind, 
		rx_claim_id_src,
		member_id_src,
		table_id_src,
		load_date,
		oop,
		quantity
		)
select 'iqva' as data_source,
	   a.year as year,
	   b.uth_rx_claim_id as uth_rx_claim_id,
	   b.uth_member_id as uth_member_id,
	   a.from_dt as fill_date,
       a.ndc as ndc,
       a.dayssup as days_supply,
	   a.month_id as month_year_id,
	   a.generic_name as generic_name,
	   a.product_name as brand_name,
	   a.prscbr_id as provider_npi, 
	   a.allowed as	total_allowed_amount, 
	   a.paid as total_paid_amount,
	   a.deductible as deductible, 
	   a.copay as copay, 
	   a.coinsamt as coins, 
	   a.cobamt as cob,
	   public.get_fy_from_date(a.from_dt) as fiscal_year,
	   a.thptc_clas_id as therapeutic_class,
	   a.daw as dispensed_as_written,
	   a.dosage_form_nm as dose, 
	   a.strength as strength, 
	   a.formulary as formulary_ind, 
	   a.derv_claimno as rx_claim_id_src,
	   a.pat_id as member_id_src,
	   'claims' as table_id_src, 
	   current_date as load_date,
	   a.oop as oop,
	   a.quan as quantity
from staging_clean.iqva_rx_etl a 
  join staging_clean.iqva_dim_rx_claim_id b
     on b.member_id_src = a.pat_id
    and b.rx_claim_id_src = a.derv_claimno; 

-- Vacuum analyze: 
select 'Vacuum analyze dw_staging.iqva_pharmacy_claims started at: ' || current_timestamp as message;
analyze dw_staging.iqva_pharmacy_claims ;

-- Grant Access:
grant select on dw_staging.iqva_pharmacy_claims to uthealth_analyst;


-- Final timestamp:
select 'IQVIA pharmacy claims script completed at: ' || current_timestamp as message;



-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

-- View table:
--select * from dw_staging.iqva_pharmacy_claims;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Compare row counts (they should be the same):
select count(*) from staging_clean.iqva_rx_etl; -- CNT: 3312774549
select count(*) from dev.sa_iqvia_derv_claimno_new_all_yr where new_rectype = 'P'; -- CNT: 3312774549
select count(*) from dw_staging.iqva_pharmacy_claims; -- CNT: 3312774549

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct year:
select distinct year from dw_staging.iqva_pharmacy_claims order by 1; -- 2006 thru 2023


