/* This script is for testing code for the local SQL server before bringing it into R
 * for dw-enrollment-QA
 */

/******************************************
 * Simple look at tables in question
 *****************************************/
SELECT top 5 * FROM medicaid.dbo.enrl_2012 ;

SELECT top 5 * FROM medicaid.dbo.CHIP_UTH_SFY2012_Final;

SELECT top 5 * FROM medicaid.dbo.ENRL_2018_HTW;

SELECT top 5 * FROM work.dbo.xz_mcd_reconciliation_etl;


/******************************************
 * Build table of all the enrollment data
 *****************************************/

--Initialize table
drop table if exists work.dbo.xz_mcd_reconciliation_etl;
create table work.dbo.xz_mcd_reconciliation_etl (
	CLIENT_NBR varchar(50),
	FY varchar(4),
	elig_date varchar(20),
	DOB date,
	SEX varchar(2),
	MCO varchar(50),
	RACE varchar(2),
	ZIP varchar(10),
	SMIB varchar(2),
	HTW int
);

insert into work.dbo.xz_mcd_reconciliation_etl
select a.CLIENT_NBR, '2012' as ENRL_FY, a.elig_date, try_cast(a.DOB as date) as dob, 
a.SEX, b.MCO_PROGRAM_NM as MCO, a.RACE, a.ZIP, a.SMIB,
case when a.me_code = 'W' then 1 else 0 end as HTW
from medicaid.dbo.enrl_2012 a left join medicaid.dbo.LU_Contract as b on a.CONTRACT_ID = b.PLAN_CD;

insert into work.dbo.xz_mcd_reconciliation_etl
select CLIENT_NBR, '2012' as ENRL_FY, elig_month, 
try_cast(substring(date_of_birth, 1, 9) as date) as dob, 
gender_cd, 'CHIP' as MCO, ethnicity as race, substring(MAILING_ZIP, 1, 5) as zip, '0' as SMIB,
'0' as HTW
from medicaid.dbo.CHIP_UTH_SFY2012_Final;

insert into work.dbo.xz_mcd_reconciliation_etl
select a.CLIENT_NBR, '2018' as ENRL_FY, a.elig_date, try_cast(a.DOB as date) as dob, 
a.SEX, b.MCO_PROGRAM_NM as MCO, a.RACE, a.ZIP, a.SMIB,
case when a.me_code = 'W' then 1 else 0 end as HTW
from medicaid.dbo.ENRL_2018_HTW a left join medicaid.dbo.LU_Contract as b on a.CONTRACT_ID = b.PLAN_CD;


/******************************************
 * Clean SEX variable
 *****************************************/
drop table if exists work.dbo.xz_mcd_reconciliation_sex_t;

select client_nbr, fy, sex, count(distinct elig_date) as count, max(elig_date) as recent
into work.dbo.xz_mcd_reconciliation_sex_t
from work.dbo.xz_mcd_reconciliation_etl
where sex in ('F', 'M') --clean out U or J
group by client_nbr, fy, sex;

drop table if exists work.dbo.xz_mcd_reconciliation_sex;

with cte as (select *,
  	    row_number() over (partition by client_nbr, fy order by count desc, recent desc) as row_num
	from work.dbo.xz_mcd_reconciliation_sex_t)

select client_nbr, fy, sex
into work.dbo.xz_mcd_reconciliation_sex
from cte
where row_num = 1;

--check sex: ID people with > 1 sex
with cte as (
	select client_nbr, fy, count(distinct sex) as num_sexes
	from work.dbo.xz_mcd_reconciliation_etl
	group by client_nbr, fy)
select * from cte where num_sexes > 1;

--see if the computed results match human intuition
select * from work.dbo.xz_mcd_reconciliation_etl where client_nbr = '531249141' --and fy = '2015'
order by elig_date;

select * from work.dbo.xz_mcd_reconciliation_sex where client_nbr = '531249141'


/******************************************
 * Some minor data exploration to see if we have some weird values that are out of range
 *****************************************/

--HOW MANY SEXES ARE THERE?
select sex, count(sex) from work.dbo.xz_mcd_reconciliation_etl group by sex;
--answer: F, M, U, and 3 J

--dates look pretty legit
select top 100 dob, count(dob) as count
from work.dbo.xz_mcd_reconciliation_etl
group by dob
order by count(dob) desc;

select top 100 dob, count(dob) as count
from work.dbo.xz_mcd_reconciliation_etl
group by dob
order by count(dob) desc;

select top 100 zip, count(zip) as count
from stage.dbo.AGG_ENRL_MCD_FY
group by zip
order by count(zip) desc;

select distinct smib from work.dbo.xz_mcd_reconciliation_etl;
--SMIB is strictly only 0s and 1s, no weird values

--nothing's jumping out, but actually looking at frequency counting's not going to do much


/******************************************
 * Clean DOB
 *****************************************/
drop table if exists work.dbo.xz_mcd_reconciliation_dob_t;
select client_nbr, dob, count(distinct elig_date) as count, max(elig_date) as recent
into work.dbo.xz_mcd_reconciliation_dob_t
from work.dbo.xz_mcd_reconciliation_etl
group by client_nbr, dob;

drop table if exists work.dbo.xz_mcd_reconciliation_dob;
with cte as (select *,
  	    row_number() over (partition by client_nbr order by count desc, recent desc) as row_num
	from work.dbo.xz_mcd_reconciliation_dob_t)

select client_nbr, dob
into work.dbo.xz_mcd_reconciliation_dob
from cte
where row_num = 1;


--check dob
with cte as (
	select client_nbr, count(distinct dob) as num
	from work.dbo.xz_mcd_reconciliation_etl
	group by client_nbr)
select * from cte where num > 1;

select * from work.dbo.xz_mcd_reconciliation_etl where client_nbr = '611208834'
order by elig_date;

select * from work.dbo.xz_mcd_reconciliation_dob where client_nbr = '611208834'


/******************************************
 * Clean MCO
 *****************************************/
--Check what MCOs exist
select distinct mco from work.dbo.xz_mcd_reconciliation_etl;

--Make MCO priority table
drop table if exists work.dbo.xz_mcd_mco_priority;

CREATE TABLE work.dbo.xz_mcd_mco_priority (
  plan_type varchar(20) NOT NULL,
  priority int NOT NULL
);

--add values to it
INSERT INTO work.dbo.xz_mcd_mco_priority(plan_type, priority)
  VALUES ('CHIP', 1 ),
	('STAR Kids', 2 ),
	('STAR+PLUS', 2 ),
	('STAR Health', 2 ),
	('STAR', 3 ),
	('MMP', 4 ),
	('FFS', 4 ),
	('PCCM', 4 );
                
--clean MCO
drop table if exists work.dbo.xz_mcd_reconciliation_mco_t;
select a.client_nbr, a.fy, a.mco, b.priority, count(distinct a.elig_date) as count, max(a.elig_date) as recent
into work.dbo.xz_mcd_reconciliation_mco_t
from work.dbo.xz_mcd_reconciliation_etl a left join work.dbo.xz_mcd_mco_priority b on a.mco = b.plan_type
group by a.client_nbr, a.fy, a.mco, b.priority;

drop table if exists work.dbo.xz_mcd_reconciliation_mco;
with cte as (select *,
  	    row_number() over (partition by client_nbr, fy order by count desc, priority, recent desc) as row_num
	from work.dbo.xz_mcd_reconciliation_mco_t)

select client_nbr, fy, mco
into work.dbo.xz_mcd_reconciliation_mco
from cte
where row_num = 1;

--check MCO
with cte as (
	select client_nbr, fy, count(distinct mco) as num_mco
	from work.dbo.xz_mcd_reconciliation_etl
	group by client_nbr, fy)
select top 100 * from cte where num_mco > 1;

select * from work.dbo.xz_mcd_reconciliation_etl where client_nbr = '606434193'
order by elig_date;

select * from work.dbo.xz_mcd_reconciliation_mco where client_nbr = '606434193'


/******************************************
 * Clean ZIP, STATE, SMIB, HTW
 *****************************************/

--clean zip code
drop table if exists work.dbo.xz_mcd_reconciliation_zip_t;
select client_nbr, fy, zip, count(distinct elig_date) as count, max(elig_date) as recent
into work.dbo.xz_mcd_reconciliation_zip_t
from work.dbo.xz_mcd_reconciliation_etl
group by client_nbr, fy, zip;

drop table if exists work.dbo.xz_mcd_reconciliation_zip;
with cte as (select *,
  	    row_number() over (partition by client_nbr, fy order by count desc, recent desc) as row_num
	from work.dbo.xz_mcd_reconciliation_zip_t)

select client_nbr, fy, zip
into work.dbo.xz_mcd_reconciliation_zip
from cte
where row_num = 1;

/******************************************
 * Clean ZIP, STATE, SMIB, HTW
 *****************************************/

select top 5 * from work.dbo.xz_mcd_enrl_reconciled_t;

--calculate enrolled_months and condense down to one row per client_nbr per fy
drop table if exists work.dbo.xz_mcd_enrl_reconciled_t;
select CLIENT_NBR as mem_id, FY, count(distinct elig_date) as enrolled_months
into work.dbo.xz_mcd_enrl_reconciled_t
from work.dbo.xz_mcd_reconciliation_etl
group by client_nbr, fy;

--join in half the data (do it in 2 operations to decrease server load)
drop table if exists work.dbo.xz_mcd_enrl_reconciled_t1;
select a.*, b.dob, c.sex, d.race
into work.dbo.xz_mcd_enrl_reconciled_t1
from work.dbo.xz_mcd_enrl_reconciled_t a
	left join work.dbo.xz_mcd_reconciliation_dob b on a.mem_id  = b.client_nbr
	left join work.dbo.xz_mcd_reconciliation_sex c on a.mem_id = c.client_nbr and a.fy = c.fy
	left join work.dbo.xz_mcd_reconciliation_race d on a.mem_id = d.client_nbr and a.fy = d.fy;

--join in other half of the data
drop table if exists work.dbo.xz_mcd_enrl_reconciled;
select a.*, e.zip, f.mco, g.smib, h.htw
into work.dbo.xz_mcd_enrl_reconciled
from work.dbo.xz_mcd_enrl_reconciled_t1 a
	left join work.dbo.xz_mcd_reconciliation_zip e on a.mem_id = e.client_nbr and a.fy = e.fy
	left join work.dbo.xz_mcd_reconciliation_mco f on a.mem_id = f.client_nbr and a.fy = f.fy
	left join work.dbo.xz_mcd_reconciliation_smib g on a.mem_id = g.client_nbr and a.fy = g.fy
	left join work.dbo.xz_mcd_reconciliation_htw h on a.mem_id = h.client_nbr and a.fy = h.fy;

--drop temp tables
drop table if exists work.dbo.xz_mcd_enrl_reconciled_t;
drop table if exists work.dbo.xz_mcd_enrl_reconciled_t1;


/***********************************************
 * Info from data_warehouse gets imported in R
 ***********************************************/

/***********************************************
 * Comparison for each variable
 ***********************************************/

--Make boolean table of matches

select a.member_id_src as member_id, a.fiscal_year,
	case when b.mem_id is null then 1 else 0 end as member_id_mismatch,
	case when b.fy is null then 1 else 0 end as fy_mismatch,
	case when a.total_enrolled_months != b.enrolled_months then 1 else 0 end as em_mismatch,
	a.total_enrolled_months as tacc_em,
	b.enrolled_months as spc_em
from work.dbo.xz_dwqa_temp1 a
	left join work.dbo.xz_mcd_enrl_reconciled b
	on a.member_id_src = b.mem_id and a.fiscal_year = b.fy

select top 5 * from work.dbo.xz_dwqa_temp1;

select * from work.dbo.xz_mcd_reconciliation_etl where CLIENT_NBR = '526423364' and fy = 2014
order by elig_date;



/******************************************************
 * AUGH, NEED TO CONVERT EVERYTHING TO CALENDAR YEAR
 *****************************************************/
--Initialize table
drop table if exists work.dbo.xz_mcd_reconciliation_cy_etl;
create table work.dbo.xz_mcd_reconciliation_cy_etl (
	CLIENT_NBR varchar(50),
	CY varchar(4),
	elig_date varchar(20),
	DOB date,
	SEX varchar(2),
	MCO varchar(50),
	RACE varchar(2),
	ZIP varchar(10),
	SMIB varchar(2),
	HTW int
);

insert into work.dbo.xz_mcd_reconciliation_cy_etl
select a.CLIENT_NBR, substring(a.ELIG_DATE, 1, 4) as CY, a.elig_date, try_cast(a.DOB as date) as dob, 
a.SEX, b.MCO_PROGRAM_NM as MCO, a.RACE, a.ZIP, a.SMIB,
case when a.me_code = 'W' then 1 else 0 end as HTW
from medicaid.dbo.enrl_2012 a left join medicaid.dbo.LU_Contract as b on a.CONTRACT_ID = b.PLAN_CD;

insert into work.dbo.xz_mcd_reconciliation_cy_etl
select CLIENT_NBR, substring(elig_month, 1, 4) as CY, elig_month, 
try_cast(substring(date_of_birth, 1, 9) as date) as dob, 
gender_cd, 'CHIP' as MCO, ethnicity as race, substring(MAILING_ZIP, 1, 5) as zip, '0' as SMIB,
'0' as HTW
from medicaid.dbo.CHIP_UTH_SFY2012_Final;

-- make final table

select top 5 * from work.dbo.xz_mcd_enrl_cy_reconciled_t;

--calculate enrolled_months and condense down to one row per client_nbr per cy
drop table if exists work.dbo.xz_mcd_enrl_cy_reconciled_t;
select CLIENT_NBR as mem_id, cy, count(distinct elig_date) as enrolled_months
into work.dbo.xz_mcd_enrl_cy_reconciled_t
from work.dbo.xz_mcd_reconciliation_cy_etl
group by client_nbr, cy;

--join in half the data (do it in 2 operations to decrease server load)
drop table if exists work.dbo.xz_mcd_enrl_cy_reconciled_t1;
select a.*, b.dob, c.sex, d.race
into work.dbo.xz_mcd_enrl_reconciled_cy_t1
from work.dbo.xz_mcd_enrl_reconciled_cy_t a
	left join work.dbo.xz_mcd_reconciliation_cy_dob b on a.mem_id  = b.client_nbr
	left join work.dbo.xz_mcd_reconciliation_cy_sex c on a.mem_id = c.client_nbr and a.cy = c.cy
	left join work.dbo.xz_mcd_reconciliation_cy_race d on a.mem_id = d.client_nbr and a.cy = d.cy;

--join in other half of the data
drop table if exists work.dbo.xz_mcd_enrl_cy_reconciled;
select a.*, e.zip, f.mco, g.smib, h.htw
into work.dbo.xz_mcd_enrl_cy_reconciled
from work.dbo.xz_mcd_enrl_cy_reconciled_t1 a
	left join work.dbo.xz_mcd_reconciliation_cy_zip e on a.mem_id = e.client_nbr and a.cy = e.cy
	left join work.dbo.xz_mcd_reconciliation_cy_mco f on a.mem_id = f.client_nbr and a.cy = f.cy
	left join work.dbo.xz_mcd_reconciliation_cy_smib g on a.mem_id = g.client_nbr and a.cy = g.cy
	left join work.dbo.xz_mcd_reconciliation_cy_htw h on a.mem_id = h.client_nbr and a.cy = h.cy;

--drop temp tables
drop table if exists work.dbo.xz_mcd_enrl_cy_reconciled_t;
drop table if exists work.dbo.xz_mcd_enrl_cy_reconciled_t1;

/***********************************************
 * Comparison for each variable using CALENDAR YEAR
 ***********************************************/

--Make boolean table of matches
drop table if exists work.dbo.xz_mcd_enrl_mismatches;

select a.member_id_src as member_id, a.year,
	case when b.mem_id is null then 1 else 0 end as member_id_mismatch,
	case when a.total_enrolled_months != b.enrolled_months then 1 else 0 end as em_mismatch,
	case when a.gender_cd != b.sex then 1 else 0 end as sex_mismatch,
	case when a.race_cd_src != b.race and b.race is not null then 1 else 0 end as race_mismatch,
	case when a.dob_derived != b.dob then 1 else 0 end as dob_mismatch,
	case when a.zip5 != b.zip then 1 else 0 end as zip_mismatch,
	case when a.plan_type != b.mco then 1 else 0 end as plan_mismatch,
	case when a.dual !=  b.smib then 1 else 0 end as dual_mismatch,
	case when a.htw != b.htw then 1 else 0 end as htw_mismatch,
	a.gender_cd as tacc_sex, b.sex as spc_sex,
	a.race_cd_src as tacc_race, b.race as spc_race,
	a.zip5 as tacc_zip, b.zip as spc_zip,
	a.plan_type as tacc_plan, b.mco as spc_plan
into work.dbo.xz_mcd_enrl_mismatches
from work.dbo.xz_dwqa_temp1 a
	left join work.dbo.xz_mcd_enrl_cy_reconciled b
	on a.member_id_src = b.mem_id and a.year = b.cy;
	
select sum(member_id_mismatch) as member_id_mismatches,
	sum(em_mismatch) as em_mismatches,
	sum(sex_mismatch) as sex_mismatches,
	sum(race_mismatch) as race_mismatches,
	sum(dob_mismatch) as dob_mismatches,
	sum(zip_mismatch) as zip_mismatches,
	sum(plan_mismatch) as plan_mismatches,
	sum(dual_mismatch) as dual_mismatches,
	sum(htw_mismatch) as htw_mismatches
from work.dbo.xz_mcd_enrl_mismatches	

select *
from work.dbo.xz_mcd_enrl_mismatches
where sex_mismatch = 1;

select *
from work.dbo.xz_mcd_enrl_mismatches
where race_mismatch = 1;

select *
from work.dbo.xz_mcd_enrl_mismatches
where zip_mismatch = 1;

select *
from work.dbo.xz_mcd_enrl_mismatches
where plan_mismatch = 1;

select top 5 * from work.dbo.xz_dwqa_temp1;

select top 5 * from work.dbo.xz_mcd_enrl_cy_reconciled;

--700024111	2018
select * from work.dbo.xz_mcd_reconciliation_cy_etl where CLIENT_NBR = '700024111' and cy = 2018
order by elig_date;

select * from work.dbo.xz_mcd_reconciliation_cy_etl where CLIENT_NBR = '700024111'
order by elig_date;

select * from medicaid.dbo.enrl_2018 where CLIENT_NBR = '700024111'

201709
201710
201711
201712

select * from medicaid.dbo.enrl_2019 where CLIENT_NBR = '700024111' order by elig_date;

201811
201812
201901
201902
201903
201904
201905
201906
201907

select * from medicaid.dbo.CHIP_UTH_SFY2018_Final where CLIENT_NBR = '700024111' order by elig_month;


