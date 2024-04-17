/*************************************************************************************************************
 * Script Purpose | Prepares IQVIA data to be inserted into the data_warehouse.claim_header table.
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
select 'IQVIA claim header script started at: ' || current_timestamp as message;


--=== Create empty claim_header table for IQVIA: ===--

-- Drop and create table:
drop table if exists dw_staging.iqva_claim_header;

create table dw_staging.iqva_claim_header
(like data_warehouse.claim_header including defaults) 
with (
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
) distributed by (uth_member_id);

-- Vacuum analyze:
vacuum analyze dw_staging.iqva_claim_header;



--=== Insert into dw_staging.iqva_claim_header from staging_clean.iqva_etl: ===--

-- Timestamp: 
select 'Inserting data into dw_staging.iqva_claim_header from staging_clean.iqva_etl started at: ' || current_timestamp as message;

with iqvia_agg_cte as (
	select pat_id, 
		   derv_claimno,
		   min(year) as year,
		   min(claim_type) as claim_type, -- if claim_type is F on any line, then the min will be F, otherwise P
		   min(from_dt) as from_dt,
		   max(to_dt) as to_dt,
		   sum(coalesce(allowed, 0)) as allowed, 
		   sum(coalesce(paid, 0)) as paid,
		   sum(coalesce(deductible, 0)) as deductible,
		   sum(coalesce(copay, 0)) as copay,
		   sum(coalesce(coinsamt, 0)) as coinsamt,
		   sum(coalesce(oop, 0)) as oop, 
		   max(rend_id) as rend_id, 
		   max(bill_id) as bill_id, 
		   max(bill_spec) as bill_spec 
    from staging_clean.iqva_etl
    group by pat_id, derv_claimno
)
insert into dw_staging.iqva_claim_header(
	data_source, 
	year, 
	uth_member_id,
	uth_claim_id, 
	claim_type, 
	from_date_of_service,
	to_date_of_service,
	total_allowed_amount, 
	total_paid_amount, 
	fiscal_year, 
	bill_provider,
	perf_rn_provider,
	claim_id_src,
	member_id_src,
	table_id_src,
	load_date,
	deductible,
	copay,
	coins,
	oop,
	provider_specialty
)
select 'iqva' as data_source,
       year as year,
       b.uth_member_id as uth_member_id,
       b.uth_claim_id as uth_claim_id,
       a.claim_type as claim_type,
       a.from_dt as from_date_of_service,
       a.to_dt as to_date_of_service,
       a.allowed as total_allowed_amount,
       a.paid as total_paid_amount,
       public.get_fy_from_date(a.from_dt) as fiscal_year,
       a.bill_id as bill_provider,
       a.rend_id as perf_rn_provider,
       a.derv_claimno as claim_id_src,
       a.pat_id as member_id_src,
       'claims' as table_id_src,
       current_date as load_date,
       a.deductible as deductible,
       a.copay as copay,
       a.coinsamt as coins,
       a.oop as oop,
       a.bill_spec as provider_specialty
from iqvia_agg_cte a
	join staging_clean.iqva_dim_claim_id b 
		on a.pat_id = b.member_id_src 
	   and a.derv_claimno = b.claim_id_src
where year in (2020,2021,2022,2023); -- remove when done
	
-- Vacuum analyze:
select 'Vacuum analyze dw_staging.iqva_claim_header started at: ' || current_timestamp as message;
vacuum analyze dw_staging.iqva_claim_header;

-- Grant Access:
grant select on dw_staging.iqva_claim_header to uthealth_analyst;


-- Final timestamp:
select 'IQVIA claim header script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

-- View Data:
--select * from dw_staging.iqva_claim_header order by member_id_src, claim_id_src;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Check row count by year and distinct claim count by year. Counts should match up:
select 'dw_staging.iqva_claim_header row count: ' as message;
select year, count(*) from dw_staging.iqva_claim_header group by year order by 1; -- CNT: 

select 'dw_staging.iqva_claim_header claim count: ' as message;
select year, count (distinct claim_id_src) from dw_staging.iqva_claim_header group by year order by 1; -- CNT: 
select year, count (distinct uth_claim_id) from dw_staging.iqva_claim_header group by year order by 1; -- CNT:  

select 'staging_clean.iqva_etl claim count: ' as message;
select year, count (distinct derv_claimno) from (select min(year) as year, pat_id, derv_claimno from staging_clean.iqva_etl group by pat_id, derv_claimno)a group by year order by 1; -- CNT: 

select 'IQVIA raw data claim count: ' as message;
select year, count(distinct derv_claimno) from 
	(select min(year) as year, pat_id, derv_claimno from dev.sa_iqvia_derv_claimno_new_all_yr where (new_rectype != 'P' or new_rectype is null) group by pat_id, derv_claimno)a
group by year order by 1; -- CNT: 

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct years:
select distinct year from dw_staging.iqva_claim_header order by 1; -- 2006 thru 2023

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Ensure there are no dupe rows:
--select count(*) from dw_staging.iqva_claim_header; -- CNT: 4105423335
--select count(*) from (select distinct * from dw_staging.iqva_claim_header)a; -- CNT: 4105423335






