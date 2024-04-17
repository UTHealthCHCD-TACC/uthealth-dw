
/***********************************************************************************************************************************************************************************************************
 * 
 * 
 * Logic for generating IQVIA derv_claimno and derv_linenum
 *
 * 
 ************************************************************************************************************************************************************************************************************/

--==== derv_claimno and derv_linenum Creation: ====--

-- Drop Existing Table:
drop table if exists dev.sa_iqvia_derv_claimno_new_all_yr;

-- Create Table:
create table dev.sa_iqvia_derv_claimno_new_all_yr with (
	appendonly=true,
	orientation=column,
	compresstype=zlib
) 
as 
select *
from (
	select row_number() over (partition by pat_id, derv_claimno) as derv_linenum, * 
	from (
		select 
			case 
				when from_dt != to_dt and (claimno is null or claimno = '') and (conf_num is not null or conf_num != '') then concat(pat_id, conf_num) -- #1. claims with diff from and to dt with a conf_num
				when from_dt != to_dt and (claimno is null or claimno = '') and (conf_num is null or conf_num = '') and (new_rectype != 'P' or new_rectype is null) then concat(pat_id, replace(from_dt, '-', ''), replace(to_dt, '-', ''), new_rectype)-- #2. claims with diff from and to dt without a conf_num (non pharm)
				when from_dt != to_dt and (claimno is null or claimno = '') and (conf_num is null or conf_num = '') and new_rectype = 'P' then concat(pat_id, replace(from_dt, '-', ''), replace(to_dt, '-', ''), ndc) -- #3. claims with diff from and to dt without a conf_num (pharm)
				when from_dt = to_dt and (claimno is null or claimno = '') and new_rectype = 'P' then concat(pat_id, replace(from_dt, '-', ''), ndc) -- #4. pharm claims 
				when from_dt = to_dt and (claimno is null or claimno = '') and (new_rectype != 'P' or new_rectype is null) then concat(pat_id, replace(from_dt, '-', ''), new_rectype) -- #5. same day medical/non-pharmacy claims
				when claimno is not null or claimno != '' then concat(pat_id, claimno) -- #6. for lines with claimno (should solve issue where claimno is tied to multiple pat_ids)
			end as derv_claimno, *	
		from(
			select 
				case 
					when (rectype = 'P' or (rectype is null and pos = '01')) and ((proc_cde is not null or proc_cde != '') or (rev_code is not null or rev_code != '')) then 'PM' -- PM ("pharm-medical")
					when (rectype = 'P' or (rectype is null and pos = '01')) and ((proc_cde is null or proc_cde = '') and (rev_code is null or rev_code = '')) then 'P' 
					else rectype
				end as new_rectype, *
			from iqvia.claims 
		)a 
	)b
)c 
order by pat_id, derv_claimno, derv_linenum
distributed by (pat_id);

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Grant Access:
grant select on dev.sa_iqvia_derv_claimno_new_all_yr to uthealth_analyst;


--= Quick Checks: =--

-- Ensure there are no null derv_claimnos:
select * from dev.sa_iqvia_derv_claimno_new_all_yr where derv_claimno is null; -- no nulls

--Ensure iqvia.claims and dev.sa_iqvia_derv_claimno_new_all_yr have the same number of rows:
select count(*) from dev.sa_iqvia_derv_claimno_new_all_yr; -- 12418694960
select count(*) from iqvia.claims; -- 12418694960

-- Ensure years are from 2006 - 2023:
select distinct substring(from_dt,1,4) from dev.sa_iqvia_derv_claimno_new_all_yr order by 1; -- 2006 thru 2023

-- Ensure there are 7 new_rectypes:
select distinct new_rectype from dev.sa_iqvia_derv_claimno_new_all_yr order by 1 desc; -- 7 new rectypes: P, PM (new), A, S, F, M, and null

-- Ensure there are 6 new_rectypes left after filtering out where new_rectype != 'P' or new_rectype is null (this will be used for the medical claims table):
select distinct new_rectype from dev.sa_iqvia_derv_claimno_new_all_yr where new_rectype != 'P' or new_rectype is null order by 1 desc; -- 6 new rectypes: PM, A, S, F, M, and null

-- Ensure there is 1 new_rectype left after filtering for where new_rectype = 'P' (this will be used for the pharmacy claims table):
select distinct new_rectype from dev.sa_iqvia_derv_claimno_new_all_yr where new_rectype = 'P' order by 1 desc; -- P
select distinct rectype from dev.sa_iqvia_derv_claimno_new_all_yr where new_rectype = 'P' order by 1 desc; -- P and null, as expected






