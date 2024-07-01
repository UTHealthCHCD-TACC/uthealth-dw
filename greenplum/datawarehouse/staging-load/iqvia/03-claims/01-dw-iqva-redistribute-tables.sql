/*************************************************************************************************************
 * Script Purpose | Redistributes the data_warehouse.dim_uth_claim_id and IQVIA claims table.
 * _______________| Any transformations for variables are also performed in this script.
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
select 'IQVIA table redistribution and data transformation script started at: ' || current_timestamp as message;


--=== Copy the data_warehouse.dim_uth_claim_id and distribute on member_id_src and claim_id_src: ===--

-- Timestamp: 
select 'Redistributing data_warehouse.dim_uth_claim_id table for IQVIA started at: ' || current_timestamp as message;

-- Drop and create table:
drop table if exists staging_clean.iqva_dim_claim_id;
   
create table staging_clean.iqva_dim_claim_id as    
select member_id_src, claim_id_src, 
       uth_member_id, uth_claim_id
  from data_warehouse.dim_uth_claim_id  
 where data_source = 'iqva'
distributed by (member_id_src, claim_id_src);

-- Vacuum analyze:
vacuum analyze staging_clean.iqva_dim_claim_id;



--=== Redistribute the IQVIA claims table on pat_id and derv_claimno and transform variables as needed: ===--

-- Timestamp:
select 'Redistributing IQVIA claims table started at: ' || current_timestamp as message;

-- Drop and create table:
drop table if exists staging_clean.iqva_etl;

create table staging_clean.iqva_etl with (
	appendonly = true, 
	orientation = row, 
	compresstype = zlib, 
	compresslevel = 5 
) as 
with iqvia_clean_cte as( -- select and transform variables as needed here
	select pat_id,
		   derv_claimno,
		   substring(from_dt,1,4)::int as year,
		   derv_linenum::int,
		   from_dt::date,
		   to_dt::date,
		   month_id::int,
		   pos,
		   case 
			   when patstat = 'UN' then '0' -- maps patstat to 0 when patstat = UN (unknown)
			   when length(trim(patstat)) = 3 then replace(patstat, '~', '') -- removes ~ from patstat when length of patstat = 3 (specifically handles when patstat is ~02)
			   when patstat ~ '[A-Z]' then null -- maps patstat to null when patstat contains letters
			   when patstat !~ '[0-9]' then null -- maps patstat to null when patstat contains special characters (EX: ??, \\)
			   else patstat 
		   end as patstat,
		   regexp_replace(proc_cde, '[^a-zA-Z0-9]', '', 'g') as proc_cde, -- remove non-alphanumeric characters from proc_cde
		   cpt_mod,
		   case 
			   when rev_code in ('000', '0000') then null -- map rev_code to null when rev_code is 000 or 0000 
			   when length(trim(rev_code)) in (1, 2) then null -- map rev_code to null when rev_code is 1 or 2 characters long 
			   when substring(rev_code, 1, 1) not in ('0', '1', '2', '3') then null -- map invalid rev_code to null (handles ???, ? or when the first value is a letter) 
			   when substring(rev_code, 2, 1) not in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') then null -- map invalid rev_code to null (handles '0 0' or when the second value is a letter) 
			   when substring(rev_code, 3, 1) not in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') then null -- map invalid rev_code to null 
			   when length(trim(rev_code)) = '3' then lpad(rev_code, 4, '0') -- leftpad with 0 if length is 3 
			   when substring(rev_code, 4, 1) not in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') then null -- map invalid rev_code to null (handles when last value is a letter)
			   else rev_code -- will map rev_code as is when the length is 4 characters and all characters are valid 
		   end as rev_code_cleaned,
		   allowed::numeric,
		   paid::numeric,
		   copay::numeric,
		   deductible::numeric,
		   coinsamt::numeric,
		   cobamt::numeric,
		   coalesce(coinsamt::numeric, 0) + coalesce(deductible::numeric, 0) + coalesce(copay::numeric, 0) as oop,
		   case 
			   when length(trim(billtype)) = 1 then null -- map to null when length is 1 
			   when substring(billtype, 1, 1) not in ('1', '2', '3', '4', '5', '6', '7', '8', '9') then null -- map invalid billtype inst to null (handles ???, ~1, ??6, ??X, N, P, E, 0PY, etc.)
			   when substring(billtype, 2, 1) not in ('1', '2', '3',' 4', '5', '6', '7', '8', '9') then null -- map invalid billtype class to null
			   when length(trim(billtype)) = 2 then rpad(billtype, 3, '1') -- right pads with 1 when billtype is 2 digits long 
			   when substring(billtype, 3, 1) = '?' then replace(billtype, '?', '1') -- replaces ? with 1 when billtype ends with ? 
			   when substring(billtype, 3, 1) not in ('0', '1', '2', '3', '4', '5', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'M', 'O', 'P', 'Q', 'X', 'Y', 'Z') then null -- map invalid billtype freq to null 
			   else billtype -- will map billtype as is when length is 3 and all characters in billtype are valid
	       end as billtype_cleaned,
	       srv_unit::float8,
	       bill_id,
	       rend_id,  
	       case 
		       when diagprc_ind = '-1' then null
		       when diagprc_ind = '1' then '9'
		       when diagprc_ind = '2' then '0'
		       else null
		   end as diagprc_ind,
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
		   new_rectype,
		   bill_spec
	from dev.sa_iqvia_derv_claimno -- iqvia.claims table with the generated derv_claimnos
	where (new_rectype != 'P' or new_rectype is null) -- filter for where new_rectype != P or where new_rectype is null to select medical claims only
)
select *,
	case 
	   	when (billtype_cleaned is not null or b.revenue_cd is not null) or (billtype_cleaned != '' or b.revenue_cd != '') then 'F' -- determine claim_type on cleaned bill type (billtype_cleaned) and revenue code (b.revenue_cd)
	   	when ((billtype_cleaned is null and b.revenue_cd is null) or (billtype_cleaned = '' and b.revenue_cd = '')) and new_rectype in ('F', 'S') then 'F' -- if both the cleaned values for bill type (billtype_cleaned) and revenue code (b.revenue_cd) are null, use new_rectype to determine claim_type
	   	else 'P'
	end as claim_type,
	billtype_cleaned as billtype,
	b.revenue_cd as rev_code
from iqvia_clean_cte a
	left join reference_tables.ref_revenue_code b 
		on a.rev_code_cleaned = b.revenue_cd -- left join to the revenue code reference table to retrieve valid revenue codes. Use values in the b.revenue_cd col as rev_code to retain valid revenue codes
distributed by (pat_id, derv_claimno);

-- Vacuum analyze: 
vacuum analyze staging_clean.iqva_etl;


-- Final timestamp:
select 'IQVIA table redistribution and data transformation script completed at: ' || current_timestamp as message;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--= Various Checks =--

/*

-- View table:
--select * from staging_clean.iqva_etl order by pat_id, derv_claimno, derv_linenum limit 5000;
--select * from staging_clean.iqva_etl where claim_type = 'F' order by pat_id, derv_claimno, derv_linenum limit 5000;

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Counts (make sure IQVIA raw data row count and staging_clean.iqva_etl row count match):
select 'iqvia raw claims row count: ' as message, count(*) 
from dev.sa_iqvia_derv_claimno where (new_rectype != 'P' or new_rectype is null); -- CNT: 9255962780

select 'staging_clean.iqva_etl row count: ' as message, count(*) from staging_clean.iqva_etl; -- CNT: 9255962780

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Counts (make sure data_warehouse.dim_uth_claim_id row count and staging_clean.iqva_dim_claim_id row count match):
select 'staging_clean.iqva_dim_claim_id row count: ' as message, count(*) from staging_clean.iqva_dim_claim_id; -- CNT: 4170185799
select 'data_warehouse.dim_uth_claim_id row count: ' as message, count(*) from data_warehouse.dim_uth_claim_id where data_source = 'iqva'; -- CNT: 4170185799

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

-- Distinct length of billtype (should be 3):
select 'distinct length of billtype: ' as message;
select distinct(length(billtype)) from staging_clean.iqva_etl; -- null, 3

-- Distinct length of rev_code (should be 4):
select 'distinct length of rev_code: ' as message;
select distinct(length(rev_code)) from staging_clean.iqva_etl; -- null, 4
 
-- Distinct new_rectype (Should be NULL, PM, A, S, F, M):
select 'distinct new_rectype: ' as message;
select distinct new_rectype from staging_clean.iqva_etl; -- null, A, PM, M, F, S

-- Distinct years (Should be 2006 thru 2023):
select 'distinct year: ' as message;
select distinct year from staging_clean.iqva_etl order by 1; -- 2006 thru 2023

--````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

--== Logic Check: ==--

-- Find Distinct claim_type when billtype is not null or rev_code is not null (should be F):
select 'distinct claim_type when billtype or rev_code is not null: ' as message;
select distinct claim_type 
from staging_clean.iqva_etl where (billtype is not null or rev_code is not null) or (billtype != '' or rev_code != ''); -- F

--select * from staging_clean.iqva_etl where (billtype is not null or rev_code is not null) or (billtype != '' or rev_code != '') order by pat_id, derv_claimno, derv_linenum; -- F, view data


-- Find Distinct claim_type when billtype is null and rev_code is null and new_rectype = F or S (should be F):
select 'distinct claim_type when billtype and rev_code is null: ' as message;
select distinct claim_type 
from staging_clean.iqva_etl where ((billtype is null and rev_code is null) or (billtype = '' and rev_code = '')) and new_rectype in ('F', 'S'); -- F

--select * from staging_clean.iqva_etl where ((billtype is null and rev_code is null) or (billtype = '' and rev_code = '')) and new_rectype in ('F', 'S') order by pat_id, derv_claimno, derv_linenum; -- F, view data


-- Find Distinct claim_type when billtype is null and rev_code is null and new_rectype != F or S (should be P):
select 'distinct claim_type when billtype and rev_code is null and new_rectype is not F or S: ' as message;
select distinct claim_type 
from staging_clean.iqva_etl where ((billtype is null and rev_code is null) or (billtype = '' and rev_code = '')) and (new_rectype not in ('F', 'S') or new_rectype is null); -- P

--select * 
--from staging_clean.iqva_etl 
--where ((billtype is null and rev_code is null) or (billtype = '' and rev_code = '')) and (new_rectype not in ('F', 'S') or new_rectype is null) order by pat_id, derv_claimno, derv_linenum; -- P view data

-- Ensure proc_cde does not contain non-alphanumeric characters:
select * from staging_clean.iqva_etl where proc_cde ~ '[^a-zA-Z0-9]'; -- no rows returned
select * from iqvia.claims where proc_cde ~ '[^a-zA-Z0-9]' limit 5; -- raw data for comparison

*/

