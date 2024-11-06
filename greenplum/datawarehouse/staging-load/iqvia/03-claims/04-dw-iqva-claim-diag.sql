/*************************************************************************************************************
 * Script Purpose | Prepares IQVIA data to be inserted into the data_warehouse.claim_diag table.
 * _______________| 
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 1/19/23  || Sharrah   || Script created.
 * ---------++-----------++------------------------------------------------------------------------------------
 * 4/16/24  || Sharrah   || Updated script to remove decimals from Dx codes
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           || 
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA claim diag script started at: ' || current_timestamp as message;

/*
--=== Create empty claim_diag table for IQVIA: ===--

-- Drop and create table:
drop table if exists dw_staging.iqva_claim_diag; 

create table dw_staging.iqva_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
) distributed by (uth_member_id);

-- Vacuum analyze:
vacuum analyze dw_staging.iqva_claim_diag;

*/

--=== Insert into dw_staging.iqva_claim_diag from staging_clean.iqva_etl: ===--

-- Timestamp:
select 'Inserting data into dw_staging.iqva_claim_diag from staging_clean.iqva_etl started at: ' || current_timestamp as message;

insert into dw_staging.iqva_claim_diag( 
data_source, 
uth_member_id, 
uth_claim_id, 
from_date_of_service,
diag_cd, 
diag_position, 
icd_version,
load_date,
year,
fiscal_year,
claim_id_src,
member_id_src,
table_id_src
)  					
with 
diag_cd_cte as (
	select  
		pat_id,
		derv_claimno, 
		diag_admit,
		diag1,
		diag2,
		diag3,
		diag4,
		diag5,
		diag6,
		diag7,
		diag8,
		diag9,
		diag10,
		diag11,
		diag12,
		diagprc_ind
   from staging_clean.iqva_etl
   group by pat_id, derv_claimno, diag_admit,
			diag1, diag2, diag3, diag4, diag5,
			diag6, diag7, diag8, diag9, diag10,
			diag11, diag12, diagprc_ind
),
date_cte as (
  	select 
  		pat_id,
  		derv_claimno,
  		min(from_dt) as from_dt,
  		min(year) as year
  	from staging_clean.iqva_etl
    group by pat_id, derv_claimno 
),
diag_unnest_cte as(
	select 
		pat_id,
		derv_claimno,
		unnest(array[diag_admit, diag1, diag2, diag3, diag4, diag5, diag6, diag7, diag8, diag9, diag10, diag11, diag12]) as diag_cd,
 	 	unnest(array['A', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']) as diag_position,
 	 	diagprc_ind as icd_version
 	from diag_cd_cte
)
select distinct * from ( -- distinct * to remove instances of duplicate codes per position in the diag_cd col for a claim
	select 
 	 	'iqva' as data_source,
 	 	c.uth_member_id as uth_member_id,
 	 	c.uth_claim_id as uth_claim_id,
 	 	b.from_dt as from_date_of_service,
 	 	replace(a.diag_cd, '.', '') as diag_cd, -- use replace() to remove any "." from Dx codes
 	 	a.diag_position as diag_position,
 	 	case
			when b.from_dt >= '2016-01-01' then min(a.icd_version) over (partition by c.uth_member_id, c.uth_claim_id, a.diag_cd, a.diag_position) -- min will select 0 (10) as the version (diag_cds with only one ICD version will not be affected as the min and max will be the same ICD version regardless of the date)
			when b.from_dt < '2016-01-01' then max(a.icd_version) over (partition by c.uth_member_id, c.uth_claim_id, a.diag_cd, a.diag_position) -- max will select 9 as the version (diag_cds with only one ICD version will not be affected as the min and max will be the same ICD version regardless of the date)
	 	end as icd_version,
 	 	current_date as load_date,
 	 	b.year as year,
 	 	public.get_fy_from_date(b.from_dt) as fiscal_year,
 	 	a.derv_claimno as claim_id_src,
 	 	a.pat_id as member_id_src,
 	 	'claims' as table_id_src	
	from diag_unnest_cte a
		join date_cte b 
  			on a.pat_id = b.pat_id 
  		   and a.derv_claimno = b.derv_claimno
		join staging_clean.iqva_dim_claim_id c 
  			on c.member_id_src = a.pat_id 
  		   and c.claim_id_src = a.derv_claimno
)dx 
where diag_cd is not null
and year in (2023); -- remove when done

-- Vacuum analyze:
select 'Vacuum analyze dw_staging.iqva_claim_diag started at: ' || current_timestamp as message;
vacuum analyze dw_staging.iqva_claim_diag;

-- Grant Access:
grant select on dw_staging.iqva_claim_diag to uthealth_analyst;


-- Final timestamp:
select 'IQVIA claim diag script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

-- View table:
--select * from dw_staging.iqva_claim_diag;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````
 
-- Row count:
select 'dw_staging.iqva_claim_diag row count: ' as message, count(*) from dw_staging.iqva_claim_diag; -- CNT: 9143765048

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct icd version:
select distinct icd_version from dw_staging.iqva_claim_diag; -- null, 9, 10

-- Distinct year:
select distinct year from dw_staging.iqva_claim_diag order by 1; -- 2006 thur 2023


-- Ensure a Dx code only has one ICD version:

/*
select uth_claim_id, diag_cd, diag_position, count(distinct icd_version) 
from dw_staging.iqva_claim_diag 
group by uth_claim_id, diag_cd, diag_position 
having count(distinct icd_version) > 1; -- no rows returned
*/

