/*************************************************************************************************************
 * Script Purpose | Prepares IQVIA data to be inserted into the data_warehouse.claim_icd_proc table.
 * _______________| 
 *
 *
 * Change Log
 *-------------------------------------------------------------------------------------------------------------
 * Date  	|| Author    || Notes
 * ---------++-----------++------------------------------------------------------------------------------------
 * 1/09/23  || Sharrah   || Script created.
 * ---------++-----------++------------------------------------------------------------------------------------
 * 4/16/24  || Sharrah   || Updated script to remove non-alphanumeric characters from procedure codes
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           || 
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA claim ICD proc script started at: ' || current_timestamp as message;


--=== Create empty claim_icd_proc table for IQVIA: ===--

-- Drop and create table:
drop table if exists dw_staging.iqva_claim_icd_proc;

create table dw_staging.iqva_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
)distributed by (uth_member_id);

-- Vacuum analyze:
vacuum analyze dw_staging.iqva_claim_icd_proc;



--=== Insert into dw_staging.iqva_claim_icd_proc from staging_clean.iqva_etl: ===--

-- Timestamp:
select 'Inserting data into dw_staging.iqva_claim_icd_proc from staging_clean.iqva_etl started at: ' || current_timestamp as message;

insert into dw_staging.iqva_claim_icd_proc( 
data_source, 
uth_member_id, 
uth_claim_id, 
from_date_of_service,
proc_cd, 
proc_position,
icd_version,
load_date,
year,
fiscal_year,
claim_id_src,
member_id_src,
table_id_src
)  					
with 
proc_cd_cte as (
	select 
		pat_id,
		derv_claimno, 
		icdprc1,
		icdprc2,
		icdprc3,
		icdprc4,
		icdprc5,
		icdprc6,
		icdprc7,
		icdprc8,
		icdprc9,
		icdprc10,
		icdprc11,
		icdprc12,
		diagprc_ind
   from staging_clean.iqva_etl
   group by pat_id, derv_claimno,
		    icdprc1, icdprc2, icdprc3, icdprc4, icdprc5,
		    icdprc6, icdprc7, icdprc8, icdprc9, icdprc10,
		    icdprc11, icdprc12, diagprc_ind
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
proc_unnest_cte as(
	select 
		a.pat_id,
		a.derv_claimno,
		b.from_dt,
		b.year,
		unnest(array[a.icdprc1, a.icdprc2, a.icdprc3, a.icdprc4, a.icdprc5, a.icdprc6, a.icdprc7, a.icdprc8, a.icdprc9, a.icdprc10, a.icdprc11, a.icdprc12]) as proc_cd,
 	 	unnest(array[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]) as proc_position,
 	 	a.diagprc_ind as icd_version
 	from proc_cd_cte a
 		join date_cte b 
  			on a.pat_id = b.pat_id 
  		   and a.derv_claimno = b.derv_claimno
  	--where year in (2022,2023)-- for loading in batches by year
)
select distinct * from ( -- distinct * to remove instances of duplicate codes per position in the proc_cd col for a claim
	select 
 		'iqva' as data_source,
 	 	c.uth_member_id as uth_member_id,
 	 	c.uth_claim_id as uth_claim_id,
 	 	a.from_dt as from_date_of_service,
  	 	upper(regexp_replace(a.proc_cd, '[^a-zA-Z0-9]', '', 'g')) as proc_cd, -- use regexp_replace() to remove any non-alphanumeric characters from ICD proc codes
 	 	a.proc_position as proc_position,
 	 	case
			when a.from_dt >= '2016-01-01' then min(a.icd_version) over (partition by c.uth_member_id, c.uth_claim_id, a.proc_cd, a.proc_position) -- min will select 0 (10) as the version (proc_cds with only one ICD version will not be affected as the min and max will be the same ICD version regardless of the date)
			when a.from_dt < '2016-01-01' then max(a.icd_version) over (partition by c.uth_member_id, c.uth_claim_id, a.proc_cd, a.proc_position) -- max will select 9 as the version (proc_cds with only one ICD version will not be affected as the min and max will be the same ICD version regardless of the date)
	 	end as icd_version,
 	 	current_date as load_date,
 	 	a.year as year,
 	 	public.get_fy_from_date(a.from_dt) as fiscal_year,
 	 	a.derv_claimno as claim_id_src,
 	 	a.pat_id as member_id_src,
 	 	'claims' as table_id_src	
	from proc_unnest_cte a
		join staging_clean.iqva_dim_claim_id c 
  			on c.member_id_src = a.pat_id 
  		   and c.claim_id_src = a.derv_claimno
)x 
where proc_cd is not null and proc_cd != '';

-- Vacuum analyze:
select 'Vacuum analyze dw_staging.iqva_claim_icd_proc started at: ' || current_timestamp as message;
vacuum analyze dw_staging.iqva_claim_icd_proc;

-- Grant Access:
grant select on dw_staging.iqva_claim_icd_proc to uthealth_analyst;


-- Final timestamp:
select 'IQVIA claim ICD proc script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

/*

-- View table:
--select * from dw_staging.iqva_claim_icd_proc;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Row count:
select 'dw_staging.iqva_claim_icd_proc row count: ' as message, count(*) from dw_staging.iqva_claim_icd_proc; -- CNT: 45797103
select count(*) from (select distinct * from dw_staging.iqva_claim_icd_proc)a; -- Used to ensure there are no dupes (since I loaded in batches, this is to ensure I didn't accidently load a year 2x). CNT: 45797103

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct icd version:
select distinct icd_version from dw_staging.iqva_claim_icd_proc; -- 0, 9, null

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct year:
select distinct year from dw_staging.iqva_claim_icd_proc order by 1; -- 2006 thru 2023

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Check for nulls:
select * from dw_staging.iqva_claim_icd_proc where member_id_src is null or member_id_src = ''; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_icd_proc where claim_id_src is null or claim_id_src  = ''; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_icd_proc where uth_claim_id  is null; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_icd_proc where uth_member_id is null; -- no rows returned, no nulls

select * from dw_staging.iqva_claim_icd_proc where proc_cd = ''; -- no rows returned
select * from dw_staging.iqva_claim_icd_proc where proc_cd is null; -- no rows returned, no nulls

-- Check for non-alphanumeric characters:
select * from dw_staging.iqva_claim_icd_proc where proc_cd ~ '[^a-zA-Z0-9]'; -- no rows returned

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Ensure a proc code only has one ICD version:
select uth_claim_id, proc_cd, proc_position, count(distinct icd_version) 
from dw_staging.iqva_claim_icd_proc 
group by uth_claim_id, proc_cd, proc_position 
having count(distinct icd_version) > 1; -- no rows returned

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Compare DW to new load - ensure they are the same:
select * from dw_staging.iqva_claim_icd_proc where claim_id_src  = 'ximh8tzsfkect3facilmvhjrbikb9892' and member_id_src = 'ximh8tzsfkect3fa' order by proc_position;
select * from data_warehouse.claim_icd_proc where data_source = 'iqva' and claim_id_src  = 'ximh8tzsfkect3facilmvhjrbikb9892' and member_id_src = 'ximh8tzsfkect3fa' order by proc_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = 'ximh8tzsfkect3facilmvhjrbikb9892' and pat_id = 'ximh8tzsfkect3fa';

select * from dw_staging.iqva_claim_icd_proc where claim_id_src  = '4o5i4sd4710vc1ayvx16aqclsw0z0kto' and member_id_src = '4o5i4sd4710vc1ay' order by proc_position;
select * from data_warehouse.claim_icd_proc where data_source = 'iqva' and claim_id_src  = '4o5i4sd4710vc1ayvx16aqclsw0z0kto' and member_id_src = '4o5i4sd4710vc1ay' order by proc_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = '4o5i4sd4710vc1ayvx16aqclsw0z0kto' and pat_id = '4o5i4sd4710vc1ay';

select * from dw_staging.iqva_claim_icd_proc where claim_id_src  = 'szbej7wfay08b8xfa8jcqfydmya433td' and member_id_src = 'szbej7wfay08b8xf' order by proc_position;
select * from data_warehouse.claim_icd_proc where data_source = 'iqva' and claim_id_src  = 'szbej7wfay08b8xfa8jcqfydmya433td' and member_id_src = 'szbej7wfay08b8xf' order by proc_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = 'szbej7wfay08b8xfa8jcqfydmya433td' and pat_id = 'szbej7wfay08b8xf';

select * from dw_staging.iqva_claim_icd_proc where claim_id_src  = '489qhlp40z27kmfbwn7qzkl6q46hvad7' and member_id_src = '489qhlp40z27kmfb' order by proc_position;
select * from data_warehouse.claim_icd_proc where data_source = 'iqva' and claim_id_src  = '489qhlp40z27kmfbwn7qzkl6q46hvad7' and member_id_src = '489qhlp40z27kmfb' order by proc_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = '489qhlp40z27kmfbwn7qzkl6q46hvad7' and pat_id = '489qhlp40z27kmfb';

select * from dw_staging.iqva_claim_icd_proc where claim_id_src  = '5hxje2r5wbar7idwqkr7lvbobtgqbie4' and member_id_src = '5hxje2r5wbar7idw' order by proc_position;
select * from data_warehouse.claim_icd_proc where data_source = 'iqva' and claim_id_src  = '5hxje2r5wbar7idwqkr7lvbobtgqbie4' and member_id_src = '5hxje2r5wbar7idw' order by proc_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = '5hxje2r5wbar7idwqkr7lvbobtgqbie4' and pat_id = '5hxje2r5wbar7idw';

*/

