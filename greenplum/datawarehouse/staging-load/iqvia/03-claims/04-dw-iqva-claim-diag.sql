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
 * 4/16/24  || Sharrah   || Updated script to remove non-alphanumeric characters from Dx codes
 * ---------++-----------++------------------------------------------------------------------------------------
 *          ||           || 
 **************************************************************************************************************/ 

-- Timestamp:
select 'IQVIA claim diag script started at: ' || current_timestamp as message;


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
		a.pat_id,
		a.derv_claimno,
		b.from_dt,
		b.year,
		unnest(array[a.diag_admit, a.diag1, a.diag2, a.diag3, a.diag4, a.diag5, a.diag6, a.diag7, a.diag8, a.diag9, a.diag10, a.diag11, a.diag12]) as diag_cd,
 	 	unnest(array['A', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']) as diag_position,
 	 	a.diagprc_ind as icd_version
 	from diag_cd_cte a
 		join date_cte b 
  			on a.pat_id = b.pat_id 
  		   and a.derv_claimno = b.derv_claimno
  	--where year in (2022,2023) -- for loading in batches by year
)
select distinct * from ( -- distinct * to remove instances of duplicate codes per position in the diag_cd col for a claim
	select 
 	 	'iqva' as data_source,
 	 	c.uth_member_id as uth_member_id,
 	 	c.uth_claim_id as uth_claim_id,
 	 	a.from_dt as from_date_of_service,
 	 	upper(regexp_replace(a.diag_cd, '[^a-zA-Z0-9]', '', 'g')) as diag_cd, -- use regexp_replace() to remove any non-alphanumeric characters from Dx codes
 	 	a.diag_position as diag_position,
 	 	case
			when a.from_dt >= '2016-01-01' then min(a.icd_version) over (partition by c.uth_member_id, c.uth_claim_id, a.diag_cd, a.diag_position) -- min will select 0 (10) as the version (diag_cds with only one ICD version will not be affected as the min and max will be the same ICD version regardless of the date)
			when a.from_dt < '2016-01-01' then max(a.icd_version) over (partition by c.uth_member_id, c.uth_claim_id, a.diag_cd, a.diag_position) -- max will select 9 as the version (diag_cds with only one ICD version will not be affected as the min and max will be the same ICD version regardless of the date)
	 	end as icd_version,
 	 	current_date as load_date,
 	 	a.year as year,
 	 	public.get_fy_from_date(a.from_dt) as fiscal_year,
 	 	a.derv_claimno as claim_id_src,
 	 	a.pat_id as member_id_src,
 	 	'claims' as table_id_src	
	from diag_unnest_cte a
		join staging_clean.iqva_dim_claim_id c 
  			on c.member_id_src = a.pat_id 
  		   and c.claim_id_src = a.derv_claimno
)dx 
where diag_cd is not null and diag_cd != '';

-- Vacuum analyze:
select 'Vacuum analyze dw_staging.iqva_claim_diag started at: ' || current_timestamp as message;
vacuum analyze dw_staging.iqva_claim_diag;

-- Grant Access:
grant select on dw_staging.iqva_claim_diag to uthealth_analyst;


-- Final timestamp:
select 'IQVIA claim diag script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

/*

-- View table:
--select * from dw_staging.iqva_claim_diag;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````
 
-- Row count:
select 'dw_staging.iqva_claim_diag row count: ' as message, count(*) from dw_staging.iqva_claim_diag; -- CNT: 9323811134 
select count(*) from (select distinct * from dw_staging.iqva_claim_diag)a; -- Used to ensure there are no dupes (since I loaded in batches, this is to ensure I didn't accidently load a year 2x). CNT: 9323811134

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct icd version:
select distinct icd_version from dw_staging.iqva_claim_diag; -- null, 9, 10

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct year:
select distinct year from dw_staging.iqva_claim_diag order by 1; -- 2006 thur 2023

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Check for nulls:
select * from dw_staging.iqva_claim_diag where member_id_src is null or member_id_src = ''; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_diag where claim_id_src is null or claim_id_src  = ''; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_diag where uth_claim_id  is null; -- no rows returned, no nulls
select * from dw_staging.iqva_claim_diag where uth_member_id is null; -- no rows returned, no nulls

select * from dw_staging.iqva_claim_diag where diag_cd = ''; -- no rows returned
select * from dw_staging.iqva_claim_diag where diag_cd is null; -- no rows returned, no nulls

-- Check for non-alphanumeric characters:
select * from dw_staging.iqva_claim_diag where diag_cd ~ '[^a-zA-Z0-9]'; -- no rows returned

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Ensure a Dx code only has one ICD version:
select uth_claim_id, diag_cd, diag_position, count(distinct icd_version) 
from dw_staging.iqva_claim_diag 
group by uth_claim_id, diag_cd, diag_position 
having count(distinct icd_version) > 1; -- no rows returned

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Compare DW to new load - ensure they are the same:
select * from dw_staging.iqva_claim_diag where claim_id_src  = 'qq2mzaofi6ojz1cq98nj5lnxznaqq80b' and member_id_src = 'qq2mzaofi6ojz1cq' order by diag_position;
select * from data_warehouse.claim_diag where data_source = 'iqva' and claim_id_src  = 'qq2mzaofi6ojz1cq98nj5lnxznaqq80b' and member_id_src = 'qq2mzaofi6ojz1cq' order by diag_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = 'qq2mzaofi6ojz1cq98nj5lnxznaqq80b' and pat_id = 'qq2mzaofi6ojz1cq';

select * from dw_staging.iqva_claim_diag where claim_id_src  = '5khq3pujbnhvu4drl9rd3jgq8vhm5cxq' and member_id_src = '5khq3pujbnhvu4dr' order by diag_position;
select * from data_warehouse.claim_diag where data_source = 'iqva' and claim_id_src  = '5khq3pujbnhvu4drl9rd3jgq8vhm5cxq' and member_id_src = '5khq3pujbnhvu4dr' order by diag_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = '5khq3pujbnhvu4drl9rd3jgq8vhm5cxq' and pat_id = '5khq3pujbnhvu4dr';

select * from dw_staging.iqva_claim_diag where claim_id_src  = '058cotpph0hmo1lo35unu7wc2esef4f9' and member_id_src = '058cotpph0hmo1lo' order by diag_position;
select * from data_warehouse.claim_diag where data_source = 'iqva' and claim_id_src  = '058cotpph0hmo1lo35unu7wc2esef4f9' and member_id_src = '058cotpph0hmo1lo' order by diag_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = '058cotpph0hmo1lo35unu7wc2esef4f9' and pat_id = '058cotpph0hmo1lo';

select * from dw_staging.iqva_claim_diag where claim_id_src  = 'xsprbk8t0zapd16wn08fdysftog3qy1b' and member_id_src = 'xsprbk8t0zapd16w' order by diag_position;
select * from data_warehouse.claim_diag where data_source = 'iqva' and claim_id_src  = 'xsprbk8t0zapd16wn08fdysftog3qy1b' and member_id_src = 'xsprbk8t0zapd16w' order by diag_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = 'xsprbk8t0zapd16wn08fdysftog3qy1b' and pat_id = 'xsprbk8t0zapd16w';

select * from dw_staging.iqva_claim_diag where claim_id_src  = 'f0013nvkee6b5i5020060222PM' and member_id_src = 'f0013nvkee6b5i50' order by diag_position;
select * from data_warehouse.claim_diag where data_source = 'iqva' and claim_id_src  = 'f0013nvkee6b5i5020060222PM' and member_id_src = 'f0013nvkee6b5i50' order by diag_position;
select * from dev.sa_iqvia_derv_claimno where derv_claimno = 'f0013nvkee6b5i5020060222PM' and pat_id = 'f0013nvkee6b5i50';

*/

