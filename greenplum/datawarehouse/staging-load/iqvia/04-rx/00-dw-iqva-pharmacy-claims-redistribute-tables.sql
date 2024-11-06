/*************************************************************************************************************
 * Script Purpose | Redistributes the data_warehouse.dim_uth_rx_claim_id and IQVIA claims table.
 * _______________| Any transformations for variables are also performed in this script.
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 2/09/23  || Sharrah   || Script created.
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           || 
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA Rx table redistribution and data transformation script started at: ' || current_timestamp as message;


--=== Copy the data_warehouse.dim_uth_rx_claim_id and distribute on member_id_src and claim_id_src: ===--

-- Timestamp: 
select 'Redistributing data_warehouse.dim_uth_rx_claim_id table for IQVIA started at: ' || current_timestamp as message;

-- Drop and create table:
drop table if exists staging_clean.iqva_dim_rx_claim_id;

create table staging_clean.iqva_dim_rx_claim_id with (
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
) as 
select rx_claim_id_src, 
       member_id_src,
       uth_rx_claim_id,
       uth_member_id 
  from data_warehouse.dim_uth_rx_claim_id
 where data_source = 'iqva'
distributed by (rx_claim_id_src);

-- Vacuum Analyze:
vacuum analyze staging_clean.iqva_dim_rx_claim_id;



--=== Redistribute the IQVIA claims table on pat_id and derv_claimno and transform variables as needed: ===--

-- Timestamp:
select 'Redistributing iqvia.claims started at: ' || current_timestamp as message;

-- Drop and create table:
drop table if exists staging_clean.iqva_rx_etl;

create table staging_clean.iqva_rx_etl with (
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
) as 
with 
iqvia_clean_cte as (
	select 
		pat_id,
		derv_claimno, 
		substring(from_dt, 1, 4)::int as year,
		derv_linenum::int,
		from_dt::date,
		dayssup::int2,
		month_id::int,
		prscbr_id,
		allowed::numeric,
		paid::numeric,
		copay::numeric,
		deductible::numeric,
		coinsamt::numeric,
		cobamt::numeric,
		coalesce(coinsamt::numeric, 0) + coalesce(deductible::numeric, 0) + coalesce(copay::numeric, 0) as oop,
		daw,
		formulary,
		quan::numeric,
		proc_cde,
		cpt_mod,
		case 
			when length(trim(ndc)) in (1, 2, 3, 4, 5, 6) then null
			when length(trim(ndc)) in (7, 8, 9, 10) then lpad(ndc, 11, '0')
			else ndc
		end as ndc_cleaned
   from dev.sa_iqvia_derv_claimno_new_all_yr -- iqvia.claims table with the generated derv_claimnos
   where new_rectype = 'P' -- filter for where new_rectype = P to select pharmacy claims only
)
select 
	a.*,
	b.generic_name,
	b.thptc_clas_id,
	b.dosage_form_nm,
	b.strength,
	b.product_name,
	a.ndc_cleaned as ndc
from iqvia_clean_cte a
	left join iqvia.rx_lookup b
		on a.ndc_cleaned = b.ndc -- joined cleaned ndcs (ndc_cleaned) to ndc in iqvia Rx ref table
distributed by (pat_id, derv_claimno);

-- Vacuum analyze:
vacuum analyze staging_clean.iqva_rx_etl;


-- Final timestamp: 
select 'IQVIA Rx table redistribution and data transformation script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

-- View table:
--select * from staging_clean.iqva_rx_etl limit 1000;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Counts (make sure IQVIA raw data row count and staging_clean.iqva_rx_etl row count match):
select 'staging_clean.iqva_rx_etl row count: ' as message, count(*) from staging_clean.iqva_rx_etl; -- CNT: 3312774549
select 'iqvia raw claims row count: ' as message, count(*) from dev.sa_iqvia_derv_claimno_new_all_yr where new_rectype = 'P'; -- CNT: 3312774549

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Counts (make sure data_warehouse.dim_uth_rx_claim_id row count and dev.sa_iqvia_rx_dim_id row count match):
select 'staging_clean.iqva_dim_rx_claim_id row count: ' as message, count(*) from staging_clean.iqva_dim_rx_claim_id; -- CNT: 3233703887
select 'data_warehouse.dim_uth_rx_claim_id row count: ' as message, count(*) from data_warehouse.dim_uth_rx_claim_id where data_source = 'iqva'; -- CNT: 3233703887

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct NDC length (should be 11):
select distinct(length(ndc)) from staging_clean.iqva_rx_etl; -- 11


