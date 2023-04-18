/****************************************
 * Purpose: This script takes goes back and modifies the MONTHLY enrollment
 * table. It can only be run after claims tables have been built, though.
 * 
 * This script will
 * 		1) look for claims whose from_dos occurs when a member is NOT enrolled for a given month-year
 * 		2) From there, it creates a distinct list of member-month-year that needs to be added
 * 			to the monthly enrollment table
 * 		3) It then fills member demographics pulled from the yearly enrollment table if it exists
 * 		4) This table is then appended to the montly enrollment table with the claim_created_flag set to TRUE
*****************************************/


/**************
 * CAUTION: Current iteration of code operates using the LIVE tables
 * If running again, change code to run on dw_staging
 *************/

/*********************
 * Create list of member_ids and year-months based on monthly enrollment table
 * ******************/
drop table if exists dw_staging.claim_created_memid_yrmonth;

create table dw_staging.claim_created_memid_yrmonth as
select data_source, member_id_src, month_year_id,
	to_date((month_year_id*100+01)::text, 'yyyymmdd') as month_start,
	(to_date((month_year_id*100+01)::text, 'yyyymmdd') + interval '1 month - 1 day')::date as month_end
from data_warehouse.member_enrollment_monthly
where data_source in ('mdcd', 'mcpp', 'mhtw')
distributed by (member_id_src);

analyze dw_staging.claim_created_memid_yrmonth;

/********************
 * Select member_id, month_year_id and claims from header
 * where member is not enrolled
 *******************/
drop table if exists dw_staging.claim_created_unmapped_claims;

create table dw_staging.claim_created_unmapped_claims as

with cte as (select a.member_id_src, a.claim_id_src,
	case when extract(month from a.from_date_of_service) > 8 then extract(year from a.from_date_of_service) + 1
		else extract(year from a.from_date_of_service) end as fiscal_year,
	extract(year from a.from_date_of_service) * 100 + extract(month from a.from_date_of_service) as month_year_id
from data_warehouse.claim_header a left join dw_staging.claim_created_memid_yrmonth b
on a.member_id_src = b.member_id_src and a.from_date_of_service between b.month_start and b.month_end
where a.data_source in ('mdcd', 'mcpp', 'mhtw')
and b.member_id_src is null)

select distinct member_id_src, fiscal_year, month_year_id from cte;
--this is the list of members and enrollment month_year_ids where
--the member has a claim, but no record of being enrolled for that month-year

analyze dw_staging.claim_created_unmapped_claims;

select count(*) from dw_staging.claim_created_unmapped_claims;
--3909410 appx 4 million rows "missing" from enrollment table

--select * from dw_staging.claim_created_unmapped_claims;

/****************
 * QA
 * How many of these are just unmapped member_ids, period?

drop table if exists dw_staging.claim_created_unique_memids;

create table dw_staging.claim_created_unique_memids as
select distinct client_nbr
from ( select client_nbr from medicaid.enrl
		union all
		select client_nbr from medicaid.chip_enrl
		union all
		select client_nbr from medicaid.htw_enrl ) z;

--Updated Rows	13029774
	
select sum(case when b.client_nbr is not null then 1 else 0 end) as matching,
	sum(case when b.client_nbr is null then 1 else 0 end) as not_matching
from dw_staging.claim_created_unmapped_claims a left join dw_staging.claim_created_unique_memids b
	on a.member_id_src = b.client_nbr;
--matching	not_matching	
--2864582	1044828

--appx 75% match, another 25% do not map to any client_nbr

 ***************/

/*****************
 * Fill in member demographics based on these rules:
 * 		Based on data from yearly table (already cleaned)
 * 		Grab the row based on this heirarchy:
 * 			Exact match > Lowest min difference > More recent year		
 *****************/

--First get the minimum difference between years
drop table if exists dw_staging.claim_created_min_diff;
create table dw_staging.claim_created_min_diff as
select b.member_id_src, b.fiscal_year, c.fiscal_year as fy_to_use,
	abs(b.fiscal_year - c.fiscal_year) as difference
from dw_staging.claim_created_unmapped_claims b inner join data_warehouse.member_enrollment_fiscal_yearly c
on b.member_id_src = c.member_id_src;

--Then figure out which data row we're actually gonna use
drop table if exists dw_staging.claim_created_fy_to_use;

create table dw_staging.claim_created_fy_to_use as
with cte as (select *,
	row_number() over (partition by member_id_src, fiscal_year desc
		order by difference, fiscal_year desc) as rn
		from dw_staging.claim_created_min_diff)

select * from cte
where rn = 1;
--Updated Rows	995058

/*********
 * QA - did the FY to use table do what we wanted it to do?
 * 
select * from dw_staging.claim_created_fy_to_use where member_id_src = '613419438';

122953201	2014.0	0.0	1
214685805	2017.0	5.0	1
215344108	2019.0	0.0	1
216473803	2014.0	0.0	1
218224003	2021.0	0.0	1
220540801	2017.0	2.0	1

--214685805
select * from dw_staging.claim_created_unmapped_claims where member_id_src = '214685805' order by fiscal_year;
select * from dw_staging.mcd_member_enrollment_fiscal_yearly where member_id_src = '214685805' order by fiscal_year;
--fiscal year = 2012
select * from medicaid.enrl where client_nbr = '214685805'; --only in 2012
select * from medicaid.chip_enrl where client_nbr = '214685805'; --does not exist
select * from medicaid.htw_enrl where client_nbr = '214685805'; --does not exist

--220540801
select * from dw_staging.claim_created_unmapped_claims where member_id_src = '220540801' order by fiscal_year;
select * from dw_staging.mcd_member_enrollment_fiscal_yearly where member_id_src = '220540801' order by fiscal_year;
--fiscal year = 2019, 2021
select * from medicaid.enrl where client_nbr = '220540801'; --only in 2019,2021
select * from medicaid.chip_enrl where client_nbr = '220540801'; --does not exist
select * from medicaid.htw_enrl where client_nbr = '220540801'; --does not exist

--answer: YES

**********/

/******************
 * OK now we can pull in the appropriate data from the FY table into the appropriate
 * format for the monthly table
 *****************/

--first create empty shell
drop table if exists dw_staging.claim_created_enrollment_for_insert;
create table dw_staging.claim_created_enrollment_for_insert
(like data_warehouse.member_enrollment_monthly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id);

--then fill in the data
insert into dw_staging.claim_created_enrollment_for_insert (
	data_source, "year", uth_member_id, month_year_id,
	gender_cd, race_cd, dob_derived, state, zip5, zip3,
	claim_created_flag, rx_coverage, fiscal_year,
	load_date, table_id_src, member_id_src
)

select c.data_source, floor(a.month_year_id/100) as "year", c.uth_member_id,
	a.month_year_id, c.gender_cd, c.race_cd, c.dob_derived,
	c.state, c.zip5, c.zip3, true as claim_created_flag, 1 as rx_coverage,
	a.fiscal_year, current_date as load_date, 'imputed' as table_id_src, a.member_id_src
from dw_staging.claim_created_unmapped_claims a inner join dw_staging.claim_created_fy_to_use b
on a.member_id_src = b.member_id_src and a.fiscal_year = b.fiscal_year
inner join data_warehouse.member_enrollment_fiscal_yearly c
on a.member_id_src = c.member_id_src and b.fy_to_use = c.fiscal_year;
--Updated Rows	2869551

--add some columns to make calcs easier
alter table dw_staging.claim_created_enrollment_for_insert
add column yr_end_date date,
add column fy_end_date date,
add column elig_month_date date;

--fill in dates with which to do calculations
update dw_staging.claim_created_enrollment_for_insert
set yr_end_date = (year::text || '-12-31')::date,
	fy_end_date = (fiscal_year::text || '-08-31')::date,
	elig_month_date = to_date(month_year_id::text, 'yyyymm');

--calcs to figure out the age_cy, age_fy, and age_month
update dw_staging.claim_created_enrollment_for_insert
	set age_cy = extract( years from age(yr_end_date, dob_derived)),
	age_fy = extract( years from age(fy_end_date, dob_derived)),
	age_months = ((extract(months from age(elig_month_date, dob_derived))) + ((extract(years from age(elig_month_date, dob_derived))) * 12));

--drop columns now that we're done
alter table dw_staging.claim_created_enrollment_for_insert
drop column yr_end_date,
drop column fy_end_date,
drop column elig_month_date;

vacuum analyze dw_staging.claim_created_enrollment_for_insert;

--look at it and check
--select * from dw_staging.claim_created_enrollment_for_insert;

/***************
 * QA
 * Make sure there are no conflicts between this table and existing monthly table

select a.*
from dw_staging.claim_created_enrollment_for_insert a left join data_warehouse.member_enrollment_monthly b
on a.member_id_src = b.member_id_src and a.month_year_id = b.month_year_id
where b.data_source in ('mdcd', 'mcpp', 'mhtw');

--ok final QA passed

 **************/

/***************
 * Insert into data_warehouse
 **************/

insert into data_warehouse.member_enrollment_monthly
select * from dw_staging.claim_created_enrollment_for_insert;
--Updated Rows	2869551

--vacuum analyze it
vacuum analyze data_warehouse.member_enrollment_monthly;

/**************
 * Clean up
 *************/
select 'drop table if exists ' || table_schema || '.' || table_name || ';'
from information_schema."tables"
where table_schema = 'dw_staging'
and table_name like 'claim_created%'
order by table_name;

drop table if exists dw_staging.claim_created_enrollment_for_insert;
drop table if exists dw_staging.claim_created_fy_to_use;
drop table if exists dw_staging.claim_created_memid_yrmonth;
drop table if exists dw_staging.claim_created_min_diff;
drop table if exists dw_staging.claim_created_unique_memids;
drop table if exists dw_staging.claim_created_unmapped_claims;

/**************
 * In case you messed up and need to start atain

delete from data_warehouse.member_enrollment_monthly_1_prt_mdcd
where claim_created_flag = true;

 */









