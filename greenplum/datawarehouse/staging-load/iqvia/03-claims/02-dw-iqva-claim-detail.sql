/*************************************************************************************************************
 * Script Purpose | Prepares IQVIA data to be inserted into the data_warehouse.claim_detail table.
 * _______________| 
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 1/09/23  || Sharrah   || Script created.
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           || 
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA claim detail script started at: ' || current_timestamp as message;


--=== Create empty claim_detail table for IQVIA: ===--

-- Drop and create table:
drop table if exists dw_staging.iqva_claim_detail;

create table dw_staging.iqva_claim_detail
(like data_warehouse.claim_detail including defaults) 
with(
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
) distributed by (uth_member_id);

-- Vacuum analyze:
vacuum analyze dw_staging.iqva_claim_detail;



--=== Insert into dw_staging.iqva_claim_detail from staging_clean.iqva_etl: ===--

-- Timestamp:
select 'Inserting data into dw_staging.iqva_claim_detail from staging_clean.iqva_etl started at: ' || current_timestamp as message;

insert into dw_staging.iqva_claim_detail(
	data_source, 
	year, 
	uth_member_id,
	uth_claim_id, 
	claim_sequence_number,
	from_date_of_service,
	to_date_of_service,
	month_year_id,
	place_of_service,
	admit_date,
	discharge_date,
	discharge_status,
	cpt_hcpcs_cd,
	proc_mod_1,
	revenue_cd,
	allowed_amount,
	paid_amount,
	copay,
	deductible,
	coins,
	cob,
	bill_type_inst,
	bill_type_class,
	bill_type_freq,
	units,
	fiscal_year,
	table_id_src,
	bill_provider,
	perf_rn_provider,
	claim_id_src,
	member_id_src,
	load_date,
	bill,
	provider_specialty
)
select  'iqva' as data_source,
		a.year as year, 
		b.uth_member_id as uth_member_id, 
		b.uth_claim_id as uth_claim_id,
		a.derv_linenum as claim_sequence_number,
		a.from_dt as from_date_of_service,
		a.to_dt as to_date_of_service,
		a.month_id as month_year_id,
		a.pos as place_of_service,
		case 
			when substring(a.billtype, 1, 2) in ('11', '21') or substring(a.billtype, 1, 1) = '7' then a.from_dt -- mapped for inpatient ONLY - when the beginning of billtype is the following, then map to admit_date: 11, 21, 7 
			else null
		end as admit_date, 
		case 
			when substring(a.billtype, 1, 2) in ('11', '21') or substring(a.billtype, 1, 1) = '7' then a.to_dt -- mapped for inpatient ONLY - when the beginning of billtype is the following, then map to discharge_date: 11, 21, 7 
			else null
		end as discharge_date,
		a.patstat as discharge_status,
		a.proc_cde as cpt_hpcs_cd,
		a.cpt_mod as proc_mod_1,
		a.rev_code as revenue_cd,  
		a.allowed as allowed_amount,
		a.paid as paid_amount,
		a.copay as copay,
		a.deductible as deductible,
		a.coinsamt as coins,
		a.cobamt as cob,
		substring(a.billtype, 1, 1) as bill_type_inst, 
		substring(a.billtype, 2, 1) as bill_type_class,
		substring(a.billtype, 3, 1) as bill_type_freq, 
		a.srv_unit as units,
		public.get_fy_from_date(from_dt) as fiscal_year,
		'claims' as table_id_src,
		a.bill_id as bill_provider,
		a.rend_id as perf_rn_provider,
		a.derv_claimno as claim_id_src,
		a.pat_id as member_id_src,
		current_date as load_date,
		a.billtype as bill, 
		a.bill_spec as provider_specialty 
from staging_clean.iqva_etl a 
  	join staging_clean.iqva_dim_claim_id b 
    	on b.member_id_src = a.pat_id 
   	   and b.claim_id_src = a.derv_claimno;
   	  
-- Vacuum analyze: 
select 'Vacuum analyze dw_staging.iqva_claim_detail started at: ' || current_timestamp as message;
vacuum analyze dw_staging.iqva_claim_detail;

-- Grant Access:
grant select on dw_staging.iqva_claim_detail to uthealth_analyst;


-- Final timestamp:
select 'IQVIA claim detail script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

/*

-- View Table and Compare to DW claim_detail Table:
--select * from dw_staging.iqva_claim_detail order by member_id_src, claim_id_src, claim_sequence_number limit 1000;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Counts (make sure dw_staging.iqva_claim_detail row count and staging_clean.iqva_etl row count match):
select 'iqvia raw claims row count: ' as message, count(*) 
from dev.sa_iqvia_derv_claimno where (new_rectype != 'P' or new_rectype is null); -- CNT: 9255962780

select 'staging_clean.iqva_etl row count: ' as message, count(*) from staging_clean.iqva_etl; -- CNT: 9255962780

select 'dw_staging.iqva_claim_detail row count: ' as message,count(*) from dw_staging.iqva_claim_detail; -- CNT: 9255962780

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct Years: 
select 'distinct year: ' as message;
select distinct(year) from dw_staging.iqva_claim_detail order by 1; -- 2006 thru 2023

-- Distinct length of bill (should be 3):
select 'distinct length of bill: ' as message;
select distinct(length(bill)) from dw_staging.iqva_claim_detail; -- null, 3

-- Distinct length of rev_code (should be 4):
select 'distinct length of revenue_cd: ' as message;
select distinct(length(revenue_cd)) from dw_staging.iqva_claim_detail; -- null, 4

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct Admit Date and dischage date when substring(a.billtype,1,2) not in ('11', '21') or substring(a.billtype,1,1) != '7' (should be null):
select distinct admit_date from dw_staging.iqva_claim_detail where (substring(bill,1,2) not in ('11', '21')) and (substring(bill,1,1) != '7'); -- null
select distinct discharge_date from dw_staging.iqva_claim_detail where (substring(bill,1,2) not in ('11', '21')) and (substring(bill,1,1) != '7'); -- null

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct Admit Date and dischage date when substring(a.billtype,1,2) in ('11', '21') or substring(a.billtype,1,1) = '7' (should not be null):
--select distinct admit_date from dw_staging.iqva_claim_detail where substring(bill,1,2) in ('11', '21') or substring(bill,1,1) = '7'; -- not null
--select distinct discharge_date from dw_staging.iqva_claim_detail where substring(bill,1,2) in ('11', '21') or substring(bill,1,1) = '7'; -- not null

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Check for NULLS:
select * from dw_staging.iqva_claim_detail where member_id_src is null or member_id_src = ''; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_detail where claim_id_src is null or claim_id_src  = ''; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_detail where uth_claim_id  is null; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_detail where uth_member_id is null; -- no rows returned, no nulls

*/

