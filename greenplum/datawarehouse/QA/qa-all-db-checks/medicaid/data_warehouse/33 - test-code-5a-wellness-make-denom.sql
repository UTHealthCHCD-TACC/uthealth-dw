/* This SQL script is part of the set of SQL scripts used to generate numbers for
 * 5 Agency - Wellness. It is loosely based on Will's code, but has been modified for
 * the mini-data_warehouse Xiaorui made on the local SQL server.
 * 
 * Each file is meant to be run sequentially, starting from this one (0).
 * 99 - QA is a file of things Xiaorui checked while writing this code and does not need
 * to be run, but QA checks are indicated by numbers (e.g. QA check 1)
 * 
 * The archived logic plans are located in CDEXTRA$\XIAORUI\09-2022 5Agency Wellness\specs
 * Live version of enrollment logic can be found in Extract$\5-Agency_report\MEDICAID
 * (Not sure where the wellness-specific logic lives)
 * 
 * Script 0: Get denominators
 * Annual Exam: 					Denom is count of members with CE of 12 months
 * Adult Depression Screening: 		Denom is count of members with CE of 12 months and >= 18 yrs of age
 * Influenza VAccine:				Denom is count of members with CE of 12 months
 * HPV Immunization:				Denom is count of 13 year olds with CE 12 months prior to member's 13th birthday
 * Smoking Cessation:				Denom is count of persons 15+ enrolled on 12/31 of reporting year
 * Obesity and Wt Counseling:		Denom is count of persons enrolled on 12/31 of reporting year
 */

/*
select top 50 * from stage.dbo.AGG_ENRL_MCD_FYMON where ENRL_FY = 2021;
select top 500 * from stage.dbo.AGG_ENRL_MCD_FY where ENRL_FY = 2021
order by client_nbr;
*/

/*****************************************************************************
 *            Get list of members with CE of 12 months by year
 *****************************************************************************/

/*NOTE THAT WE USE AGGREGATOR FUNCTIONS HERE BECAUSE ENROLLED MONTHS IS SOMETIMES
SPLIT UP BETWEEN TWO ROWS (I.E. WHEN A MEMBER MOVES) SO IT SOMETIMES IS
REPRESENTED AS 8 MONTHS AND 4 MONTHS, ETC.

THIS CODE USES THE AGGREGATED ENROLLMENT TABLE JEFF MADE (STAGE.DBO.AGG_ENRL_MCD_FY)
WHICH ALREADY HAS CLEANED MCO AND SMIB - XIAORUI EMPIRICALLY DETERMINED ITS CLEANLINESS
THROUGH CODE THAT CAN BE FOUND IN 99 - QA.sql
for AGE/AGEGRP we take the more recent age, so max() works here
the only variable that needs cleaning is SEX */

--generate table of members with sum(enrl_months)
drop table if exists dev.xz_5a_mcd_enrl_ce;

with e (PCN, ENRL_FY, MCO, SMIB, SEX, AGE, AGEGRP, EM) as 
  (select CLIENT_NBR as PCN, ENRL_FY, min(MCO_PROGRAM_NM) as MCO, max(SMIB) as SMIB, min(SEX) as SEX, 
  	max(AGE) as AGE, max(AGEGRP) as AGEGRP, sum(ENRL_MONTHS) as EM
  from stage.dbo.AGG_ENRL_MCD_FY
  where ENRL_FY between @start_year and @end_year
  group by CLIENT_NBR, ENRL_FY )

--subset it to just members with CE >=12
select *
into dev.xz_5a_mcd_enrl_ce
from e
where EM >= 12
;

--QA check 1: SEX needs to be cleaned, all others are ok

/*******************************************************
 *      CLEANING SEX VARIABLE: SEE CDEXTRA$\XIAORUI\Medicaid DX control\Variable cleaning - sex.sql
 * 		Final table: dev.xz_mcd_enrl_sex_cleaned
 * 		This table holds reconciled sex data for everyone in Medicaid for all data up to FY2021 
 *******************************************************/

/*******************************************************
 *       THIS IS THE CLEANED ENROLLMENT TABLE
 *******************************************************/
update dev.xz_5a_mcd_enrl_ce
set sex = b.rec_sex
	from dev.xz_5a_mcd_enrl_ce a
	left join dev.xz_mcd_enrl_sex_cleaned b
	on a.pcn = b.client_nbr and a.ENRL_FY = b.enrl_fy
	where a.sex != b.rec_sex
;

/*****************************************************************************
 *          Denominators for HPV Vaccination
 *****************************************************************************/

/* More efficient to batch-process since we have 5 years of lookback
 * See HPV SQL/R-Markdown files*/

/*****************************************************************************
 *            Get list of members enrolled on 12/31
 * 				Used for smoking and obesity/weight
 *****************************************************************************/
--select top 50 * from stage.dbo.AGG_ENRL_MCD_FYMON where ENRL_FY = 2021;
--ok cool we have 2021 loaded

drop table if exists dev.xz_5a_mcd_enrl_dec;

select member_id_src as PCN, plan_type as MCO, gender_cd as sex, age_derived as age,
dual as smib, fiscal_year as enrl_fy,
case when age_derived between 0 and 19 then 1
	when age_derived between 20 and 34 then 2
	when age_derived between 35 and 44 then 3
	when age_derived between 45 and 54 then 4
	when age_derived between 55 and 64 then 5
	when age_derived between 65 and 74 then 6
	when age_derived >= 75 then 7
	else 9
	end as agegrp
into dev.xz_5a_mcd_enrl_dec
from dev.member_enrollment_fiscal_yearly
where enrolled_dec = 1
and fiscal_year between 2016 and 2021;

select * from dev.xz_5a_mcd_enrl_dec;

select CLIENT_NBR as PCN, MCO_PROGRAM_NM as MCO, SEX, AGE, SMIB, ENRL_FY, AGEGRP
into dev.xz_5a_mcd_enrl_dec
from stage.dbo.AGG_ENRL_MCD_FYMON
where substring(elig_month, 5, 2) = '12'
	and ENRL_FY between @start_year and @end_year
;



/***********************************************************************
 * CLEANED ENROLLMENT TABLE FOR PEOPLE ENROLLED IN DECEMBER OF A FY
 ***********************************************************************/
update dev.xz_5a_mcd_enrl_dec
set sex = b.rec_sex
	from dev.xz_5a_mcd_enrl_dec a
	left join dev.xz_mcd_enrl_sex_cleaned b
	on a.pcn = b.client_nbr and a.ENRL_FY = b.enrl_fy
	where a.sex != b.rec_sex;


