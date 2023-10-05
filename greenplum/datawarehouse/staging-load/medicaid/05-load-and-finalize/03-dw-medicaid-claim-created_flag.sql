/****************************************
 * Purpose: This script goes back and modifies the MONTHLY enrollment
 * table. It can only be run after claims tables have been built, though.
 * 
 * This script will
 * 		1) look for claims whose from_dos occurs when a member is NOT enrolled for a given month-year
 * 		2) From there, it creates a distinct list of member-month-year that needs to be added
 * 			to the monthly enrollment table
 * 		3) It then fills member demographics pulled from the yearly enrollment table if it exists
 * 			or claims tables (proc and header tables) otherwise
 * 		4) This table is then appended to the monthly enrollment table with the claim_created_flag set to TRUE
 * 
 * Date     | Author  | Note
 * ******************************************
 * 04/20/23 | Xiaorui | Script created
 * 09/20/23 | Xiaorui | Updated for FY22
*****************************************/

/*********************
 * Create list of member_ids and year-months based on monthly enrollment table
 * select distinct on mem_id and month-year only, not data source
 * or else we end up with a duplicate error
 * ******************/
drop table if exists dw_staging.claim_created_memid_yrmonth;

create table dw_staging.claim_created_memid_yrmonth as
select distinct member_id_src, month_year_id,
	to_date((month_year_id*100+01)::text, 'yyyymmdd') as month_start,
	(to_date((month_year_id*100+01)::text, 'yyyymmdd') + interval '1 month - 1 day')::date as month_end
from data_warehouse.member_enrollment_monthly
where data_source in ('mdcd', 'mcpp', 'mhtw')
distributed by (member_id_src);

analyze dw_staging.claim_created_memid_yrmonth;
--Updated Rows	586823622

/********************
 * Select member_id, month_year_id and claims from header
 * where member is not enrolled
 *******************/
drop table if exists dw_staging.claim_created_unmapped_claims;

create table dw_staging.claim_created_unmapped_claims as

select a.uth_member_id, a.member_id_src, a.claim_id_src,
	case when extract(month from a.from_date_of_service) > 8 then extract(year from a.from_date_of_service) + 1
		else extract(year from a.from_date_of_service) end as fiscal_year,
	extract(year from a.from_date_of_service) * 100 + extract(month from a.from_date_of_service) as month_year_id,
	0 as in_yearly_table
from data_warehouse.claim_header a left join dw_staging.claim_created_memid_yrmonth b
on a.member_id_src = b.member_id_src and a.from_date_of_service between b.month_start and b.month_end
where a.data_source in ('mdcd', 'mcpp', 'mhtw')
and b.member_id_src is null;

--figure out which of these claims has an exact match on the fiscal_yearly enrollment table
--only use medicaid partition bc none of these claims are going to end up as htw and chip peri
--claims anyway - enrollment table needed to determine that
update dw_staging.claim_created_unmapped_claims a
set in_yearly_table = case when b.member_id_src is not null then 1 else 0 end
from data_warehouse.member_enrollment_fiscal_yearly b
where b.data_source = 'mdcd' and
b.member_id_src = a.member_id_src and b.fiscal_year = a.fiscal_year;

analyze dw_staging.claim_created_unmapped_claims;

--see how many map on a matching fiscal year
select sum(in_yearly_table) as sum_in_yearly_table,
	sum(case when in_yearly_table = 0 then 1 else 0 end) as sum_not_in_yearly,
	count(*) as total_unmapped,
	sum(in_yearly_table) * 1.0 / count(*) as in_yrly_pct
from dw_staging.claim_created_unmapped_claims;
--in_yearly		not_in_yearly	total_unmapped
--	3820127		10571466		14391593  -FY 2021
-- 3982507	11245895	15228402	0.26151837861910921448  --FY 2022
select 3820127.0/14391593;
-- so about 26.5% map exactly

--select * from dw_staging.claim_created_unmapped_claims;

/****************************
 * Pull demographic information in from claims tables and clean it
 ***************************/
drop table if exists dw_staging.claim_created_not_in_yearly_etl;

--this table pulls demographics from clm tables
create table dw_staging.claim_created_not_in_yearly_etl as
select a.member_id_src, a.month_year_id, b.sex, b.dob, 
	case when c.clm_prg_cd = '100' then 'FFS' else null end as plan_type,
	'clm_proc' as table_id_src
from dw_staging.claim_created_unmapped_claims a inner join medicaid.clm_proc b
on a.member_id_src = b.pcn and a.claim_id_src = b.icn inner join medicaid.clm_header c
on a.claim_id_src = c.icn;
--Updated Rows	13658066

--this table pulls demographics from enc tables
insert into dw_staging.claim_created_not_in_yearly_etl
select a.member_id_src, a.month_year_id, b.gen, b.dob,
	c.prg as plan_type,
	'enc_proc' as table_id_src
from dw_staging.claim_created_unmapped_claims a inner join medicaid.enc_proc b
on a.member_id_src = b.mem_id and a.claim_id_src = b.derv_enc inner join medicaid.enc_header c
on a.claim_id_src = c.derv_enc;
--Updated Rows	1667007

analyze dw_staging.claim_created_not_in_yearly_etl;

--clean gender - get counts
drop table if exists dw_staging.claim_created_sex_counts;
create table dw_staging.claim_created_sex_counts as
select member_id_src, month_year_id, sex, count(*) as count
from dw_staging.claim_created_not_in_yearly_etl
group by member_id_src, month_year_id, sex;

--clean gender - get 1 row per month_year_id
drop table if exists dw_staging.claim_created_sex_cleaned;
create table dw_staging.claim_created_sex_cleaned as
with cte as (select *,
	row_number() over (partition by member_id_src, month_year_id
		order by count desc) as rn
		from dw_staging.claim_created_sex_counts)
select * from cte
where rn = 1;

--clean dob - get counts
drop table if exists dw_staging.claim_created_dob_counts;
create table dw_staging.claim_created_dob_counts as
select member_id_src, month_year_id, dob, count(*) as count
from dw_staging.claim_created_not_in_yearly_etl
group by member_id_src, month_year_id, dob;

--clean dob - get 1 row per month_year_id
drop table if exists dw_staging.claim_created_dob_cleaned;
create table dw_staging.claim_created_dob_cleaned as
with cte as (select *,
	row_number() over (partition by member_id_src, month_year_id
		order by count desc) as rn
		from dw_staging.claim_created_dob_counts)
select * from cte
where rn = 1;

--clean plan type - get counts
drop table if exists dw_staging.claim_created_plan_counts;
create table dw_staging.claim_created_plan_counts as
select member_id_src, month_year_id, plan_type, count(*) as count
from dw_staging.claim_created_not_in_yearly_etl
where plan_type is not null
group by member_id_src, month_year_id, plan_type;

--clean plan type - get 1 row per month_year_id
drop table if exists dw_staging.claim_created_plan_cleaned;
create table dw_staging.claim_created_plan_cleaned as
with cte as (select *,
	row_number() over (partition by member_id_src, month_year_id
		order by count desc) as rn
		from dw_staging.claim_created_plan_counts)
select * from cte
where rn = 1;

--make the cleaned table of demographics
drop table if exists dw_staging.claim_created_not_in_yearly;
create table dw_staging.claim_created_not_in_yearly as
select distinct on (member_id_src, month_year_id)
	*
from dw_staging.claim_created_not_in_yearly_etl;

--update sex, dob, plan type so that it matches the cleaned version
update dw_staging.claim_created_not_in_yearly a
set sex = b.sex
from dw_staging.claim_created_sex_cleaned b
where a.member_id_src = b.member_id_src and a.month_year_id = b.month_year_id
and a.sex != b.sex;
--Updated Rows	6423

update dw_staging.claim_created_not_in_yearly a
set dob = b.dob
from dw_staging.claim_created_dob_cleaned b
where a.member_id_src = b.member_id_src and a.month_year_id = b.month_year_id
and a.dob != b.dob;
--Updated Rows	6807

update dw_staging.claim_created_not_in_yearly a
set plan_type = b.plan_type
from dw_staging.claim_created_plan_cleaned b
where a.member_id_src = b.member_id_src and a.month_year_id = b.month_year_id
and a.plan_type != b.plan_type;
--Updated Rows	3728

--select * from dw_staging.claim_created_not_in_yearly;
	
/******************
 * OK now we can pull in the appropriate data from the FY table into the appropriate
 * format for the monthly table
 *****************/

--first get a list of distinct member_ids / month_year_ids
drop table if exists dw_staging.claim_created_distinct_month_year_only;
create table dw_staging.claim_created_distinct_month_year_only as
select distinct uth_member_id, member_id_src, fiscal_year, month_year_id, in_yearly_table
from dw_staging.claim_created_unmapped_claims;

--then create empty shell
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

--Fill in the data for claims with EXACT matches from fiscal_yearly table
--use only 'mdcd' partition for reasons described above
insert into dw_staging.claim_created_enrollment_for_insert (
	data_source, "year", uth_member_id, month_year_id,
	gender_cd, race_cd, dob_derived, state, zip5, zip3,
	claim_created_flag, rx_coverage, fiscal_year,
	load_date, table_id_src, member_id_src
)
select 'mdcd' as data_source, floor(a.month_year_id/100) as "year", a.uth_member_id,
	a.month_year_id, b.gender_cd, b.race_cd, b.dob_derived,
	b.state, b.zip5, b.zip3, true as claim_created_flag, 1 as rx_coverage,
	a.fiscal_year, current_date as load_date, 'imputed' as table_id_src, a.member_id_src
from dw_staging.claim_created_distinct_month_year_only a 
inner join data_warehouse.member_enrollment_fiscal_yearly b
on a.member_id_src = b.member_id_src and a.fiscal_year = b.fiscal_year
where a.in_yearly_table = 1 and b.data_source = 'mdcd';
--Updated Rows	3820127

--Fill in the data for claims where we pulled member demographics from proc and header tables
insert into dw_staging.claim_created_enrollment_for_insert (
	data_source, "year", uth_member_id, month_year_id,
	gender_cd, dob_derived, --race/state/zip are in enrollment tables only
	claim_created_flag, rx_coverage, fiscal_year,
	load_date, table_id_src, member_id_src
)
select 'mdcd' as data_source, floor(a.month_year_id/100) as "year", a.uth_member_id,
	a.month_year_id, b.sex, b.dob::date,
	true as claim_created_flag, 1 as rx_coverage,
	a.fiscal_year, current_date as load_date, b.table_id_src as table_id_src, a.member_id_src
from dw_staging.claim_created_distinct_month_year_only a 
inner join dw_staging.claim_created_not_in_yearly b
on a.member_id_src = b.member_id_src and a.month_year_id = b.month_year_id
where a.in_yearly_table = 0;

--fill in plan type for EVERYONE regardless of where the rest of their information comes from
update dw_staging.claim_created_enrollment_for_insert a
set plan_type = b.plan_type
from dw_staging.claim_created_not_in_yearly b
where a.member_id_src = b.member_id_src and a.month_year_id = b.month_year_id;

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
--select * from dw_staging.claim_created_enrollment_for_insert order by uth_member_id, month_year_id;

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

insert into data_warehouse.member_enrollment_monthly_1_prt_mdcd
select * from dw_staging.claim_created_enrollment_for_insert;
--Updated Rows	4214530

--vacuum analyze it
vacuum analyze data_warehouse.member_enrollment_monthly_1_prt_mdcd;

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

/********************************
 * change update_log (last updated 9/20/23)
 *******************************/
--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

update data_warehouse.update_log
set data_last_updated = current_date,
	details = 'Imputed records from claims appended'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_monthly';

update data_warehouse.update_log
set data_last_updated = current_date,
	details = 'Imputed records from claims appended'
where schema_name = 'data_warehouse' and table_name = 'member_enrollment_monthly_1_prt_mdcd';

/*******************************
 * How many are claim_created?
 * select year,
	sum(case when claim_created_flag is TRUE then 1 else 0 end) as claim_created,
	sum(case when claim_created_flag is TRUE then 1 else 0 end) * 1.0 / count(*) as pct
from data_warehouse.member_enrollment_monthly_1_prt_mdcd
group by 1 order by 1;

2011	238127	0.01310706964492014979
2012	640823	0.01172647242691625833
2013	643211	0.01171734544435323418
2014	378620	0.00679976207477690918
2015	349196	0.00618897982803690666
2016	392938	0.00691437307587770348
2017	356535	0.00623618248346620608
2018	331555	0.00588154799512041993
2019	278113	0.00508024122583248950
2020	237838	0.00411610369020348181
2021	237490	0.00364986494700534106
2022	130084	0.00278779222859947708
 */

/**************
 * In case you messed up and need to start again

delete from data_warehouse.member_enrollment_monthly_1_prt_mdcd
where claim_created_flag = true;

delete from data_warehouse.member_enrollment_monthly_1_prt_mhtw
where claim_created_flag = true;

delete from data_warehouse.member_enrollment_monthly_1_prt_mcpp
where claim_created_flag = true;

vacuum analyze data_warehouse.member_enrollment_monthly;

select data_source, count(*) from data_warehouse.member_enrollment_monthly
where data_source in ('mdcd', 'mcpp', 'mhtw')
group by data_source order by data_source;
mcpp	2248053
mdcd	561461033
mhtw	23114536

select data_source, count(*) from dw_staging.mcd_member_enrollment_monthly
where data_source in ('mdcd', 'mcpp', 'mhtw')
group by data_source order by data_source;
mcpp	2248053
mdcd	561461033
mhtw	23114536

 */


