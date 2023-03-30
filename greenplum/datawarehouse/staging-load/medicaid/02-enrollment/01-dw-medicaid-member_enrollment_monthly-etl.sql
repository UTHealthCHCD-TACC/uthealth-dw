/****************************************
 * Script purpose: Build ETL for Medicaid enrollment
 * Author: Xiaorui			Date: 03/28/23
 * 	
 * Logic:
 * 	Pull all rows from all enrollment tables into one table
 * 	Pull data in from reference tables
 * 		Race converted to DW codes
 * 		Medicaid plan name converted from contract_id
 * 		State from zip code
 * 	Change all values of sex not in ('M', 'F', 'U') to 'U'
 * 	Assign heirarchy
 * 	Grab only the highest priority row where elig_date is conflicting based on this priority:
 * 		CHIP Perinatal > HTW > CHIP > STAR [ANY] > FFS/MMP/PCCM
 * 	Clean DOB using most frequent > most recent rule
 * 	
 ***************************************************************/

/***********************************
 * Pull enrollment data into an ETL
 ***********************************/
drop table if exists dw_staging.medicaid_enroll_all_rows;

create table dw_staging.medicaid_enroll_all_rows (
	client_nbr text null,
	year int2 null,
	month_year_id int4 null,
	elig_date_month date null,
	sex text null,
	zip text null,
	zip3 text null,
	yr_end_date date null,
	dob date null,
	contract_id text null,
	year_fy int2 null,
	race text null,
	smib text null,
	me_code text null,
	table_id_src unknown null,
	plan_type text null,
	state text null,
	month_year_hierarchy int
)
distributed by (client_nbr);

--insert from main enrollment tables (medicaid.enrl)
insert into dw_staging.medicaid_enroll_all_rows 
select trim(a.client_nbr) as client_nbr ,
       substring(trim(elig_date),1,4)::int as year, 
       trim(elig_date)::int as month_year_id,
       (substring(trim(elig_date),5,6) || '-01-' || substring(trim(elig_date),1,4))::Date as elig_date_month,
       trim(a.sex) as sex, 
       trim(a.zip) as zip, 
       substring(a.zip,1,3) as zip3, 
       ('12-31-' || substring(elig_date,1,4))::Date as yr_end_date,
       trim(a.dob)::date as dob, 
       trim(a.contract_id) as contract_id, 
       year_fy, 
       trim(a.race) as race,
       trim(a.smib) as smib ,
       trim(a.me_code) as me_code,
	   'enrl' as table_id_src,
	   null as plan_type,
	   null as state,
	   null as month_year_hierarchy
  from medicaid.enrl  a ;

--select * from dw_staging.medicaid_enroll_etl;

analyze dw_staging.medicaid_enroll_all_rows;

--insert from CHIP enrollment tables
insert into dw_staging.medicaid_enroll_all_rows 
select trim(a.client_nbr) as client_nbr ,
       substring(trim(elig_month),1,4)::int as year, 
       trim(elig_month)::int as month_year_id,
       (substring(trim(elig_month),5,6) || '-01-' || substring(trim(elig_month),1,4))::Date as elig_month_month,
       trim(a.gender_cd) as sex, 
       substring(trim(a.mailing_zip),1,5) as zip, 
       substring(a.mailing_zip,1,3) as zip3, 
       ('12-31-' || substring(elig_month,1,4))::Date as yr_end_date,
       to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD') as dob, 
       null as contract_id, 
       year_fy, 
       trim(a.ethnicity) as race,
       null as smib,
       null as me_code,
	   'chip_enrl' as table_id_src,
	   case when chip_per_fl = 'CP' then 'CHIP PERI' else 'CHIP' end as plan_type,
	   null as state,
	   null as month_year_hierarchy
  from medicaid.chip_enrl a ;

analyze dw_staging.medicaid_enroll_all_rows;

--- insert from htw enrollment tables
insert into dw_staging.medicaid_enroll_all_rows 
select trim(a.client_nbr) as client_nbr ,
       substring(trim(elig_date),1,4)::int as year, 
       trim(elig_date)::int as month_year_id,
       (substring(trim(elig_date),5,6) || '-01-' || substring(trim(elig_date),1,4))::Date as elig_date_month,
       trim(a.sex) as sex, 
       trim(a.zip) as zip, 
       substring(a.zip,1,3) as zip3, 
       ('12-31-' || substring(elig_date,1,4))::Date as yr_end_date,
       trim(a.dob)::date as dob, 
       trim(a.contract_id) as contract_id, 
       case 
       		when substring(elig_date,5,2)::int >= 9 
       		then substring(elig_date,1,4)::int + 1
       		else substring(elig_date,1,4)::int
       	end as fiscal_year, 
       trim(a.race) as race,
       trim(a.smib) as smib ,
       trim(a.me_code) as me_code,
	   'htw_enrl' as table_id_src,
	   null as plan_type,
	   null as state,
	   null as month_year_hierarchy
  from medicaid.htw_enrl a ;

vacuum analyze dw_staging.medicaid_enroll_all_rows;

/*******************************************
 * Pull in data from various reference tables
 *******************************************/
--get plan_type
update dw_staging.medicaid_enroll_all_rows a 
   set plan_type = c.mco_program_nm
  from reference_tables.medicaid_lu_contract c 
 where c.plan_cd = a.contract_id
   and a.plan_type is null;

--determine state by zip code
update dw_staging.medicaid_enroll_all_rows a 
   set state =  z.state
  from reference_tables.ref_zip_code z 
 where a.zip = z.zip ;

--convert Medicaid race codes to DW race codes
update dw_staging.medicaid_enroll_all_rows a 
   set race = r.race_cd 
  from reference_tables.ref_race r 
 where r.race_cd_src = a.race
   and r.data_source = 'mdcd';

--Clean up sex that is not one of the permissible values
 update dw_staging.medicaid_enroll_all_rows a 
   set sex = 'U' 
   where sex not in ('F','M','U');

vacuum analyze dw_staging.medicaid_enroll_all_rows;

/************************
 * Assign hierarchy
 ***********************/
update dw_staging.medicaid_enroll_all_rows
	set month_year_hierarchy = case
	when plan_type = 'CHIP PERI' then 1
	when (me_code = 'W' or table_id_src = 'htw_enrl') then 2
	when plan_type = 'CHIP' then 3
	when plan_type like 'STAR%' then 4
	else 5 end;

vacuum analyze dw_staging.medicaid_enroll_all_rows;

select * from dw_staging.medicaid_enroll_all_rows limit 10;
/*******************************************
 * Get 1 row per member based on heirarchy
 *******************************************/
drop table if exists dw_staging.medicaid_enroll_etl;

create table dw_staging.medicaid_enroll_etl as

with cte as (select *,
	row_number() over (partition by client_nbr, month_year_id
		order by month_year_hierarchy) as rn
		from dw_staging.medicaid_enroll_all_rows)

select * from cte
where rn = 1
distributed by (client_nbr);

vacuum analyze dw_staging.medicaid_enroll_etl;

/****************************
 * Little bit of QA to see how many rows we removed
 ****************************/
/*
select count(*) from dw_staging.medicaid_enroll_etl; --586823622
select count(*) from dw_staging.medicaid_enroll_all_rows; --587515219

select (587515219 - 586823622); --691597
select 691597.0/587515219; --0.00117715588913110351

*So approx 0.1%, not too shabby*/

/*******************************************
 * Unify DOB - get most frequent or if tie, then most recent
 *******************************************/

create table dw_staging.temp_enrl_dob
	 with (appendonly=true, orientation=column)
	 as
	 select count(*), max(month_year_id) as my, client_nbr, dob
	 from dw_staging.medicaid_enroll_etl
	 group by 3, 4;

create table dw_staging.final_enrl_dob
	 with (appendonly=true, orientation=column)
	 as
	 select * , row_number() over(partition by client_nbr order by count desc, my desc) as rn
	 from dw_staging.temp_enrl_dob
	 distributed by(client_nbr);
	
update dw_staging.medicaid_enroll_etl a set dob = b.dob 
	 from dw_staging.final_enrl_dob b 
	 where a.client_nbr = b.client_nbr
	 	and a.dob != b.dob
	 	and b.rn = 1;
--Updated Rows	706261 / 587515219 = 0.00120211524256701851

drop table dw_staging.temp_enrl_dob;
drop table dw_staging.final_enrl_dob;

vacuum analyze dw_staging.medicaid_enroll_etl;

/****************

Fix: 

update dw_staging.medicaid_enroll_etl
set plan_type = 'CHIP PERI'
where plan_type = 'CHIP Perinatal';


 */