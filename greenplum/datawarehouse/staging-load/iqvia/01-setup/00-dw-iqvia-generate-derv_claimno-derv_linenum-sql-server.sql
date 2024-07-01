
/***********************************************************************************************************************************************************************************************************
 * 
 * 
 * Logic for generating IQVIA derv_claimno (MS SQL Server)
 *
 * 
 ************************************************************************************************************************************************************************************************************/

--==== Generate derv_claimno and derv_linenum: ====--

-- Drop Existing Table:
drop table if exists iqvia.dbo.claims_derv_claimno;

-- Create Table:
select * 
into iqvia.dbo.claims_derv_claimno
from (
	select 
		case 
			when from_dt != to_dt and (claimno is null or claimno = '') and (conf_num is not null and conf_num != '') then concat(pat_id, conf_num) -- #1. claims with diff from and to dt with a conf_num
			when from_dt != to_dt and (claimno is null or claimno = '') and (conf_num is null or conf_num = '') and (new_rectype != 'P' or (new_rectype = '' or new_rectype is null)) then concat(pat_id, replace(from_dt, '-', ''), replace(to_dt, '-', ''), new_rectype)-- #2. claims with diff from and to dt without a conf_num (non pharm)
			when from_dt != to_dt and (claimno is null or claimno = '') and (conf_num is null or conf_num = '') and new_rectype = 'P' then concat(pat_id, replace(from_dt, '-', ''), replace(to_dt, '-', ''), ndc) -- #3. claims with diff from and to dt without a conf_num (pharm)
			when from_dt = to_dt and (claimno is null or claimno = '') and new_rectype = 'P' then concat(pat_id, replace(from_dt, '-', ''), ndc) -- #4. pharm claims 
			when from_dt = to_dt and (claimno is null or claimno = '') and (new_rectype != 'P' or (new_rectype = '' or new_rectype is null)) then concat(pat_id, replace(from_dt, '-', ''), new_rectype) -- #5. same day medical/non-pharmacy claims
			when claimno is not null and claimno != ''  then concat(pat_id, claimno) -- #6. for lines with claimno (should solve issue where claimno is tied to multiple pat_ids)
		end as derv_claimno,
		pat_id, 
		claimno, 
		linenum, 
		rectype, 
		tos_flag, 
		pos, 
		conf_num, 
		patstat, 
		billtype, 
		ndc, 
		daw, 
		formulary, 
		dayssup, 
		quan, 
		proc_cde, 
		cpt_mod, 
		rev_code, 
		srv_unit, 
		from_dt, 
		to_dt, 
		diagprc_ind, 
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
		allowed, 
		paid, 
		deductible, 
		copay, 
		coinsamt, 
		cobamt, 
		dispense_fee, 
		bill_id, 
		bill_spec, 
		rend_id, 
		rend_spec, 
		prscbr_id, 
		prscbr_spec, 
		ptypeflg, 
		sub_tp_cd, 
		paid_dt, 
		month_id
	from(
		select 
			case 
				when (rectype = 'P' or ((rectype = '' or rectype is null) and pos = '01')) and ((proc_cde is not null and proc_cde != '') or (rev_code is not null and rev_code != '')) then 'PM' -- PM ("pharm-medical")
				when (rectype = 'P' or ((rectype = '' or rectype is null) and pos = '01')) and ((proc_cde is null or proc_cde = '') and (rev_code is null or rev_code = '')) then 'P' 
				else rectype
			end as new_rectype, *
		from 
			(
			select * from iqvia.dbo.claims_2006

			union all

			select * from iqvia.dbo.claims_2007

			union all

			select * from iqvia.dbo.claims_2008

			union all

			select * from iqvia.dbo.claims_2009

			union all

			select * from iqvia.dbo.claims_2010

			union all

			select * from iqvia.dbo.claims_2011

			union all

			select * from iqvia.dbo.claims_2012

			union all

			select * from iqvia.dbo.claims_2013

			union all

			select * from iqvia.dbo.claims_2014

			union all

			select * from iqvia.dbo.claims_2015

			union all

			select * from iqvia.dbo.claims_2016

			union all

			select * from iqvia.dbo.claims_2017

			union all

			select * from iqvia.dbo.claims_2018

			union all

			select * from iqvia.dbo.claims_2019

			union all

			select * from iqvia.dbo.claims_2020

			union all

			select * from iqvia.dbo.claims_2021

			union all

			select * from iqvia.dbo.claims_2022

			union all

			select * from iqvia.dbo.claims_2023
		)a
	)b 
)c
order by pat_id, derv_claimno;


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--= Grant Access: =--
grant select on iqvia.dbo.claims_derv_claimno_2006 to public;


--= Quick Checks: =--

/*

-- View/Compare Tables:
select top 1000 * from iqvia.dbo.claims_derv_claimno;

-- Find where pat_id = derv_claimno (WE DO NOT WANT):
select * from iqvia.dbo.claims_derv_claimno where pat_id = derv_claimno; -- no rows returned

-- Ensure there are no null derv_claimnos:
select * from iqvia.dbo.claims_derv_claimno where derv_claimno is null; -- no rows returned, no nulls
select * from iqvia.dbo.claims_derv_claimno where derv_claimno = ''; -- no rows returned, no nulls

-- Check distinct year:
select distinct substring(from_dt,1,4) from iqvia.dbo.claims_derv_claimno; -- 2006 thru 2023

-- Row count by year:
select substring(from_dt,1,4), count(*) as row_cnt
from iqvia.dbo.claims_derv_claimno
group by substring(from_dt,1,4)
order by 1;


-- derv_claimno count by year:
select substring(from_dt,1,4), count(distinct derv_claimno) as clm_cnt
from iqvia.dbo.claims_derv_claimno
group by substring(from_dt,1,4)
order by 1;

*/