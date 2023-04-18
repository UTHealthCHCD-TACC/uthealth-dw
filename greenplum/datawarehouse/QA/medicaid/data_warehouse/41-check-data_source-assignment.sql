/********************************
 * Script purpose: We recently split chip perinatal (mcpp) and healthy tx women (mhtw)
 * off into their own data sources
 * 
 * This script checks to see if data sources were correctly assigned
 ********************************/

/*******************************************
 * Grab a random sample of claims
 *******************************************/
drop table if exists qa_reporting.temp_mcd_claims;

--select random records from the claims header table where data_source = 'mdcd'
create table qa_reporting.temp_mcd_claims as
select data_source, fiscal_year, member_id_src, from_date_of_service
from data_warehouse.claim_header
where data_source = 'mdcd'
 and random() < 0.1
limit 1000;

--add in random records from 'mcpp'
insert into qa_reporting.temp_mcd_claims
select data_source, fiscal_year, member_id_src, from_date_of_service
from data_warehouse.claim_header
where data_source = 'mcpp'
 and random() < 0.1
limit 1000;

--add in random records from 'mhtw'
insert into qa_reporting.temp_mcd_claims
select data_source, fiscal_year, member_id_src, from_date_of_service
from data_warehouse.claim_header
where data_source = 'mhtw'
 and random() < 0.1
limit 1000;

--vacuum analyze it
vacuum analyze qa_reporting.temp_mcd_claims;

--check if it pulled things in correctly
select data_source, count(*) as count
from qa_reporting.temp_mcd_claims
group by data_source;
--yep it worked

/*******************************************
 * Pull enrollment data for corresponding members and fiscal years
 *******************************************/
drop table if exists qa_reporting.temp_mcd_enrl;

--from enrl
create table qa_reporting.temp_mcd_enrl as
select null::text as data_source, a.year_fy as fiscal_year,
	a.client_nbr, a.elig_date, a.smib, a.me_code, null::text as chip_per_fl,
	to_date((a.elig_date::int*100+01)::text, 'yyyymmdd') as month_start,
	(to_date((a.elig_date::int*100+01)::text, 'yyyymmdd') + interval '1 month - 1 day')::date as month_end
from medicaid.enrl a inner join qa_reporting.temp_mcd_claims b
on a.year_fy = b.fiscal_year and a.client_nbr = b.member_id_src;

--from chip
insert into qa_reporting.temp_mcd_enrl
select null::text as data_source, a.year_fy as fiscal_year,
	a.client_nbr, a.elig_month, null as smib, null as me_code, a.chip_per_fl,
	to_date((a.elig_month::int*100+01)::text, 'yyyymmdd') as month_start,
	(to_date((a.elig_month::int*100+01)::text, 'yyyymmdd') + interval '1 month - 1 day')::date as month_end	
from medicaid.chip_enrl a inner join qa_reporting.temp_mcd_claims b
on a.year_fy = b.fiscal_year and a.client_nbr = b.member_id_src;

--from htw (only for 2018, numbers might be very low)
insert into qa_reporting.temp_mcd_enrl
select null::text as data_source,
	case when substring(a.elig_date, 5, 2)::int > 8 then substring(elig_date,1,4)::int+1
		else substring(a.elig_date,1,4)::int end as fiscal_year,
	a.client_nbr, a.elig_date, a.smib, a.me_code, null::text as chip_per_fl,
	to_date((a.elig_date::int*100+01)::text, 'yyyymmdd') as month_start,
	(to_date((a.elig_date::int*100+01)::text, 'yyyymmdd') + interval '1 month - 1 day')::date as month_end
from medicaid.htw_enrl a inner join qa_reporting.temp_mcd_claims b
on	case when substring(a.elig_date, 5, 2)::int > 8 then substring(elig_date,1,4)::int+1
		else substring(a.elig_date,1,4)::int end = b.fiscal_year
	and a.client_nbr = b.member_id_src;

--assign data source
update qa_reporting.temp_mcd_enrl
set data_source = case when chip_per_fl = 'CP' then 'mcpp'
	when me_code = 'W' then 'mhtw'
	else 'mdcd' end;

--vacuum analyze
vacuum analyze qa_reporting.temp_mcd_enrl;

/******************************************
 * Check for incorrect assignments
 ******************************************/
drop table if exists qa_reporting.temp_mcd_matches;

--first let's take a look to see if there are claims that don't match enrollment data
--no plan heirarchy is applied here
--this version will throw a mismatch even if there are 2 conflicting rows
create table qa_reporting.temp_mcd_matches as
select a.member_id_src, a.fiscal_year, a.from_date_of_service,
	b.elig_date, b.month_start, b.month_end,
	a.data_source as clm_data_source, b.data_source as enrl_data_source,
	case when a.data_source = b.data_source then 0 else 1 end as data_source_mismatch
from qa_reporting.temp_mcd_claims a left join qa_reporting.temp_mcd_enrl b
on a.member_id_src = b.client_nbr
	and a.from_date_of_service between b.month_start and b.month_end;

/**************
 * Drill down and check

select * from qa_reporting.temp_mcd_matches where data_source_mismatch = 1;
member_id	fy		dos			elig_datemonth_startmonth_end	clm		enrl
522953650	2017	2016-12-21	201612	2016-12-01	2016-12-31	mcpp	mdcd -- >1 record per row
728055086	2020	2020-05-30	202005	2020-05-01	2020-05-31	mcpp	mdcd
527362429	2018	2018-03-01	201803	2018-03-01	2018-03-31	mcpp	mdcd
706302430	2019	2019-08-22	201908	2019-08-01	2019-08-31	mcpp	mdcd
523535198	2018	2017-10-09	201710	2017-10-01	2017-10-31	mcpp	mdcd

select * from qa_reporting.temp_mcd_enrl
where client_nbr = '522953650' order by elig_date;

*/

--Try it again but this time only show the ones where DW has COMPLETELY incorrect information
--e.g. there is no match from enrollment tables
drop table if exists qa_reporting.temp_mcd_matches2;

create table qa_reporting.temp_mcd_matches2 as
select a.member_id_src, a.fiscal_year, a.from_date_of_service,
	b.elig_date, b.month_start, b.month_end,
	a.data_source as clm_data_source, b.data_source as enrl_data_source,
	case when a.data_source = b.data_source then 0 else 1 end as data_source_mismatch
from qa_reporting.temp_mcd_claims a left join qa_reporting.temp_mcd_enrl b
on a.data_source = b.data_source and a.member_id_src = b.client_nbr
	and a.from_date_of_service between b.month_start and b.month_end;

/**************
 * Drill down and check

select * from qa_reporting.temp_mcd_matches2 where data_source_mismatch = 1;
--44 records here but they are all when there is no enrollment data
--all 44 records are 'mdcd' data source
member_id	fy		from_dos
519297993	2016	2016-03-25  --not enrolled in march 2016
519084348	2016	2015-12-28  --not enrolled 
520637944	2014	2014-06-24
612403706	2015	2015-06-18
616423787	2021	2020-09-21

--let's go hunt them down
select * from data_warehouse.member_enrollment_monthly
where member_id_src = '519297993' order by month_year_id;

select year_fy, client_nbr, elig_date from medicaid.enrl
where client_nbr = '519084348' order by elig_date;

*/

/***********************************
* And now that we note that there's ~40 claims per 1000 (only for Medicaid apparently)
* where there's a claim but the member is not marked as being enrolled
* soooo now we have to go back to the monthly enrollment table
* and create records for these people
* and mark them as claim_created
************************************/

/*******************************
 * Clean up
 ******************************/

drop table if exists qa_reporting.temp_mcd_claims;
drop table if exists qa_reporting.temp_mcd_enrl;
drop table if exists qa_reporting.temp_mcd_matches;
drop table if exists qa_reporting.temp_mcd_matches2;

/*******************************
 * Update: now that we've created records in the monthly table for these people
 * find out how many claims these represent
 *******************************/

/*********************
 * Create list of member_ids and year-months based on monthly enrollment table
 * INCLUDING the claim_created_flag boolean
 * ******************/
drop table if exists dw_staging.claim_created_memid_yrmonth;

create table dw_staging.claim_created_memid_yrmonth as
select data_source, claim_created_flag, member_id_src, month_year_id,
	to_date((month_year_id*100+01)::text, 'yyyymmdd') as month_start,
	(to_date((month_year_id*100+01)::text, 'yyyymmdd') + interval '1 month - 1 day')::date as month_end
from data_warehouse.member_enrollment_monthly
where data_source in ('mdcd', 'mcpp', 'mhtw')
distributed by (member_id_src);

analyze dw_staging.claim_created_memid_yrmonth;

/***********************
 * Grab a random sample of claims
 * Larger sample this time
 ***********************/

drop table if exists qa_reporting.temp_mcd_claims;

--select random records from the claims header table where data_source = 'mdcd'
create table qa_reporting.temp_mcd_claims as
select data_source, fiscal_year, member_id_src, from_date_of_service
from data_warehouse.claim_header
where data_source = 'mdcd'
 and random() < 0.1
limit 100000;

--no need to add in records from 'mcpp' or 'mhtw' because by definition
--you'd need enrollment data to be able to determine those claims

--vacuum analyze it
vacuum analyze qa_reporting.temp_mcd_claims;

--check if it pulled things in correctly
select data_source, count(*) as count
from qa_reporting.temp_mcd_claims
group by data_source;
--yep it worked

/******************************
 * Check how many have a claim created flag
 * and how many are unmatched entirely
 *****************************/

drop table if exists qa_reporting.temp_mcd_matches;

create table qa_reporting.temp_mcd_matches as
select a.data_source, a.member_id_src, a.fiscal_year, a.from_date_of_service, b.claim_created_flag,
	case when b.member_id_src is null then 1 else 0 end as unmapped_memid
from qa_reporting.temp_mcd_claims a left join dw_staging.claim_created_memid_yrmonth b
on a.member_id_src = b.member_id_src
	and a.from_date_of_service between b.month_start and b.month_end;

--select * from qa_reporting.temp_mcd_matches limit 10;

--this is how many claims generated lines rows in the monthly enrollment table
--versus how many do not have rows and in theory do not correspond to any member_id
select sum(case when claim_created_flag = true then 1 else 0 end) as claim_created,
	sum(unmapped_memid) as unmapped, count(*) as total
from qa_reporting.temp_mcd_matches;
--claim_created	unmapped	total
--		4471	0			00000

/*******************************
 * Clean up
 ******************************/

drop table if exists dw_staging.claim_created_memid_yrmonth;
drop table if exists qa_reporting.temp_mcd_claims;


select count(*) from data_warehouse.member_enrollment_monthly_1_prt_mdcd
where claim_created_flag is null;
--561461033

select count(*) from data_warehouse.member_enrollment_monthly_1_prt_mdcd
where claim_created_flag is not true;
--561461033 (equivalent)



