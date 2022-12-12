/* This SQL script is intended to adapt the code for 5 Agencies - Wellness - Medicaid
 * to use the data_warehouse so we can check to see if we're getting similar numbers
 */

/*******************************************************************
 *    Convert ICD numbers to text
 **************************************F*****************************/
alter table dev.xz_5a_dx_codes
alter column icd type varchar USING icd::varchar;

/*******************************************************************
 *    The Cohort: Members enrolled on 12/31 of reporting year
 **************************************F*****************************/

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
and fiscal_year between 2016 and 2021
and htw = 0;

--28560436 without htw
--29992191 with htw

select count(distinct pcn) from dev.xz_5a_mcd_enrl_dec
where enrl_fy = 2021;
--5,165,754

--SPC has 5,145,240 in its table wtf

/*******************************************************************
 * 	    Get members who have an obesity indication from DX codes
 *******************************************************************/
--select distinct icd_version from data_warehouse.claim_diag_1_prt_mdcd;
--select * from dev.xz_5a_dx_codes;

drop table if exists dev.xz_5a_mcd_obesity_inclusions_t;

--select included members from claims diagnoses (match ICD and DX)
select a.member_id_src as PCN, a.icd_version as ICD, a.diag_cd as DX, b.Category
into dev.xz_5a_mcd_obesity_inclusions_t
from data_warehouse.claim_diag_1_prt_mdcd a inner join dev.xz_5a_dx_codes b
on a.icd_version = b.icd and a.diag_cd like b.dx || '%'
where b.Category = 'Obesity'
and a.member_id_src in (select pcn from dev.xz_5a_mcd_enrl_dec);

--select included members from claims diagnoses (match DX only when ICD is missing)
insert into dev.xz_5a_mcd_obesity_inclusions_t
select a.member_id_src as PCN, a.icd_version as ICD, a.diag_cd as DX, b.Category
from data_warehouse.claim_diag_1_prt_mdcd a inner join dev.xz_5a_dx_codes b
on a.diag_cd like b.dx || '%'
where (a.icd_version is null or a.icd_version = '')
	and b.Category = 'Obesity'
	and a.member_id_src in (select pcn from dev.xz_5a_mcd_enrl_dec);

select count(distinct pcn) from dev.xz_5a_mcd_obesity_inclusions_t;
--1200101

/*******************************************************************
 *      Make a list of unique PCNs (members who are obese)
 *******************************************************************/

--select only distinct PCNs into inclusion cohort
drop table if exists dev.xz_5a_mcd_obesity_inclusions;

select distinct PCN
into dev.xz_5a_mcd_obesity_inclusions
from dev.xz_5a_mcd_obesity_inclusions_t
;
--THIS IS THE LIST OF MEMBERS WITH AN OBESITY INDICATION

/*******************************************************************
 *   Of obese members, find members who have received weight counseling
 * 	 from DX codes
 *******************************************************************/
drop table if exists dev.xz_5a_mcd_obesity_counseling_dx;

--select included members from claims diagnoses (match ICD and DX)
select a.member_id_src as PCN, a.icd_version as ICD, a.diag_cd as DX, b.Category
into dev.xz_5a_mcd_obesity_counseling_dx
from data_warehouse.claim_diag_1_prt_mdcd a inner join dev.xz_5a_dx_codes b
on a.icd_version = b.icd and a.diag_cd like b.dx || '%'
where b.Category = 'Weight Counseling'
and a.member_id_src in (select pcn from dev.xz_5a_mcd_obesity_inclusions);
--1571663

--select included members from claims diagnoses (match DX only when ICD is missing)
insert into dev.xz_5a_mcd_obesity_counseling_dx
select a.member_id_src as PCN, a.icd_version as ICD, a.diag_cd as DX, b.Category
from data_warehouse.claim_diag_1_prt_mdcd a inner join dev.xz_5a_dx_codes b
on a.icd_version = b.icd and a.diag_cd like b.dx || '%'
where (a.icd_version is null or a.icd_version = '')
	and b.Category = 'Weight Counseling'
	and a.member_id_src in (select pcn from dev.xz_5a_mcd_obesity_inclusions);
--0

/*******************************************************************
 *   Find members who have received weight counseling from PROC codes
 *******************************************************************/
drop table if exists dev.xz_5a_mcd_obesity_counseling_proc;

--select counseled members from claims procecure codes 
select a.member_id_src as PCN, '-' as ICD, a.proc_cd, b.Category
into dev.xz_5a_mcd_obesity_counseling_proc
from data_warehouse.claim_icd_proc_1_prt_mdcd a inner join dev.xz_5a_cpt_codes b
on a.proc_cd = b.cpt
where b.Category = 'Weight Counseling'
and a.member_id_src in (select pcn from dev.xz_5a_mcd_obesity_inclusions);
--55

/*******************************************************************
 *   Find members who have received weight counseling from DRG codes
 *   Only 3 DRGs (manual searching ok)
 * 		619 O.R. procedures for obesity with MCC 
 * 		620 O.R. procedures for obesity with CC
 *		621 O.R. procedures for obesity without CC/MCC
 *******************************************************************/

-- Select counseled members from DRG codes
drop table if exists dev.xz_5a_mcd_obesity_counseling_drg;

--select counseled members from claims procecure codes 
select PCN, DRG
into dev.xz_5a_mcd_obesity_counseling_drg
from medicaid.clm_proc
where trim(DRG) in ('619', '620', '621')
and pcn in (select pcn from dev.xz_5a_mcd_obesity_inclusions);
--510

--select counseled members from encounters procecure codes
insert into dev.xz_5a_mcd_obesity_counseling_drg
select MEM_ID, DRG
from medicaid.enc_proc
where trim(DRG) in ('619', '620', '621')
and MEM_ID in (select pcn from dev.xz_5a_mcd_obesity_inclusions);
--493

/*******************************************************************
 * Build the weight counseling cohort from DX, PROC, and DRG matches
 *******************************************************************/

--select only distinct PCNs into inclusion cohort
drop table if exists dev.xz_5a_mcd_obesity_counseling;
select distinct PCN
into dev.xz_5a_mcd_obesity_counseling
from (select PCN from dev.xz_5a_mcd_obesity_counseling_dx
	union
	select PCN from dev.xz_5a_mcd_obesity_counseling_proc
	union
	select PCN from dev.xz_5a_mcd_obesity_counseling_drg) z;
--415688
--THIS IS THE LIST OF MEMBERS WITH WEIGHT COUNSELING

/*******************************************************************
 * 			        Create results table
 *******************************************************************/
drop table if exists dev.xz_5a_mcd_obesity_results;

select a.*,
	case when b.PCN is not null then 1 else 0 end as OBESE,
	case when c.PCN is not null then 1 else 0 end as COUNSELED
into dev.xz_5a_mcd_obesity_results
from dev.xz_5a_mcd_enrl_dec a
	left join dev.xz_5a_mcd_obesity_inclusions b on a.PCN = b.PCN
	left join dev.xz_5a_mcd_obesity_counseling c on a.PCN = c.PCN;

/*******************************************************************
 * 		Create final table for output: obesity PREVALENCE
 *******************************************************************/
drop table if exists dev.xz_5a_mcd_obesity_finaltab;

select ENRL_FY as FY, MCO, 'B'::text as sex, 0 as agegrp, count(pcn) as denominator, sum(OBESE) as numerator
into dev.xz_5a_mcd_obesity_finaltab
from dev.xz_5a_mcd_obesity_results
where smib = 0
group by ENRL_FY, MCO
order by MCO;

insert into dev.xz_5a_mcd_obesity_finaltab
select ENRL_FY as FY, 'Dual Eligible' as MCO, 'B' as sex, 0 as agegrp, count(pcn) as denominator, sum(OBESE) as numerator
from dev.xz_5a_mcd_obesity_results
where smib = 1
group by ENRL_FY;

insert into dev.xz_5a_mcd_obesity_finaltab
select ENRL_FY as FY, MCO, 'B' as sex, agegrp, count(pcn) as denominator, sum(OBESE) as numerator
from dev.xz_5a_mcd_obesity_results
where smib = 0
group by ENRL_FY, MCO, agegrp
order by MCO, agegrp;

insert into dev.xz_5a_mcd_obesity_finaltab
select ENRL_FY as FY, MCO, sex, agegrp, count(pcn) as denominator, sum(OBESE) as numerator
from dev.xz_5a_mcd_obesity_results
where smib = 0 and SEX != 'U'
group by ENRL_FY, sex, MCO, agegrp
order by sex, MCO, agegrp;

/*select * from dev.xz_5a_mcd_obesity_finaltab_@yr
where sex = 'B' and agegrp = 0
order by MCO */

/*******************************************************************
 * 		Create final table for output: obesity counseling
 *******************************************************************/
drop table if exists dev.xz_5a_mcd_obesity_finaltab2;

select ENRL_FY as FY, MCO, 'B'::text as sex, 0 as agegrp, sum(OBESE) as denominator, sum(COUNSELED) as numerator
into dev.xz_5a_mcd_obesity_finaltab2
from dev.xz_5a_mcd_obesity_results
where smib = 0
group by ENRL_FY, MCO
order by MCO;

insert into dev.xz_5a_mcd_obesity_finaltab2
select ENRL_FY as FY, 'Dual Eligible' as MCO, 'B' as sex, 0 as agegrp, sum(OBESE) as denominator, sum(COUNSELED) as numerator
from dev.xz_5a_mcd_obesity_results
where smib = 1
group by ENRL_FY;

insert into dev.xz_5a_mcd_obesity_finaltab2
select ENRL_FY as FY, MCO, 'B' as sex, agegrp, sum(OBESE) as denominator, sum(COUNSELED) as numerator
from dev.xz_5a_mcd_obesity_results
where smib = 0
group by ENRL_FY, MCO, agegrp
order by MCO, agegrp;

insert into dev.xz_5a_mcd_obesity_finaltab2
select ENRL_FY as FY, MCO, sex, agegrp, sum(OBESE) as denominator, sum(COUNSELED) as numerator
from dev.xz_5a_mcd_obesity_results
where smib = 0 and SEX != 'U'
group by ENRL_FY, sex, MCO, agegrp
order by sex, MCO, agegrp;


--select * from dev.xz_5a_mcd_obesity_finaltab2
--order by fy;

---MISC TESTING AFTER HERE

select count(distinct member_id_src) from data_warehouse.member_enrollment_yearly_1_prt_mdcd
where fiscal_year = 2021
and htw = 0;
--5,699,594

select * from data_warehouse.member_enrollment_monthly_1_prt_mdcd where member_id_src = '604627839'
and fiscal_year = 2021;

select member_id_src, fiscal_year, htw from data_warehouse.member_enrollment_yearly_1_prt_mdcd where member_id_src = '604627839'
and fiscal_year = 2021;


select count(distinct member_id_src) from dev.member_enrollment_fiscal_yearly
where fiscal_year = 2021 and htw = 0;

drop table if exists dev.xz_temp2;

with cte as (select member_id_src, fiscal_year, htw from dev.member_enrollment_fiscal_yearly
	where fiscal_year = 2021 and htw = 0)

select a.*, cte.fiscal_year, cte.member_id_src, cte.htw
into dev.xz_temp2
from dev.xz_temp1 a full join cte
on a.client_nbr = cte.member_id_src and a.fy = cte.fiscal_year::text;

select count(*) from dev.xz_temp2 where client_nbr is null;
--6391

select * from dev.xz_temp2 where client_nbr is null limit 10;

612385989
715826238
742833709
743214484
739181396


select a.*, b.*
into dev.xz_temp2
from dev.xz_temp1 a full join dev.member_enrollment_fiscal_yearly b
on a.client_nbr = b.member_id_src and a.enrl_fy = b.fiscal_year
where b.fiscal_year = 2021;


select * from dev.xz_temp1;


select * from dev.xz_temp2 where client_nbr is null and htw = 0;


-----------
drop table if exists dev.xz_temp1;

select fiscal_year as fy, claim_id_src as icn, member_id_src as pcn
into dev.xz_temp1
from data_warehouse.claim_diag_1_prt_mdcd
where diag_cd like 'F32%' and
fiscal_year = 2021;

select count(*) as rows, count(distinct icn) as distinct_icn,
  count(distinct pcn) as distinct_pcn
  from dev.xz_temp1;

-- rows     icn           pcn
-- 1345318	1345318	    263930		spc
-- 1428054	1345318		263930		tacc

select * from dev.xz_temp1;

limit 100;


select fiscal_year as fy, claim_id_src, member_id_src from data_warehouse.claim_icd_proc_1_prt_mdcd
where proc_cd = '10E0XZZ' and fiscal_year = 2019;

--check these
2019	100040010201929912287166	708762463 --exists
2019	100040010201930116648001	601246448
2019	100040010202001444474230	530298054
2019	100040010202007670711045	607041237
2019	100040010202018103439684	729166574
2019	100040011201828280541627	725512951
2019	100040011201828481730108	613526093
2019	100040011201832199520169	607301731
2019	100040011201833404719028	725081688
2019	100040011201834502492154	616128699

select * from data_warehouse.claim_icd_proc_1_prt_mdcd
where claim_id_src = '100040030201930212984245';



select a.*, b.fiscal_year, b.claim_id_src, b.member_id_src, b.from_date_of_service
from dev.xz_temp4 a left join data_warehouse.claim_icd_proc_1_prt_mdcd b
on a.icn = b.claim_id_src
where a.fy::int != b.fiscal_year;


select fiscal_year, rx_claim_id_src, member_id_src
into dev.agg_enrl_mcd_cy
from data_warehouse.pharmacy_claims_1_prt_mdcd
where ndc = '';

select * from data_warehouse.member_enrollment_yearly_1_prt_mdcd
where member_id_src = '740663364';


select fiscal_year, claim_id_src, member_id_src
into dev.xz_temp1
from data_warehouse.claim_detail_1_prt_mdcd
where cpt_hcpcs_cd = '';

select a.fy as spc_fy, a.icn as spc_icn, a.pcn as spc_pcn, 
	b.fy as dw_fy, b.icn as dw_icn, b.pcn as dw_pcn
into dev.xz_temp6
from dev.xz_temp5 a full join dev.xz_temp1 b
on a.icn = b.icn
where b.icn is null;

select length(spc_icn), count(*) from dev.xz_temp6
group by length(spc_icn);

--all are length 24, therefore all are from clm tables

2021	100030010202109719699342	609517416 -- subproccd
2021	100030061202118645754421	510853621 -- subproccd
2021	100031010202105483699347	701028585 -- subproccd
2021	100031010202119746868660	707489523
2021	100031010202120747413031	521841551
2021	100031010202129470279299	744655269
2021	100031010202212544116779	530302487
2021	100031030202026930249878	505455099
2021	100031030202027632064576	529766339
2021	100031030202028234208360	511580643

select * from data_warehouse.claim_detail_1_prt_mdcd where claim_id_src = '100030010202119438302570'
--cpt_hcpcs_cd = Z9813

select proc_cd, sub_proc_cd from medicaid.clm_detail where icn = '100031010202105483699347'
--it's in sub proc cd

drop table if exists dev.xz_temp1;

select fiscal_year, claim_id_src, member_id_src
into dev.xz_temp1
from data_warehouse.claim_detail_1_prt_mdcd
where cpt_hcpcs_cd = '99213'
and fiscal_year = 2018;

select * from dev.xz_temp1 where member_id_src not in (select pcn from dev.xz_temp5);
2018	100020030201817948309523	617028738
2018	100020030201727796140850	613704031
2018	100020030201727796214073	523386402
2018	200030030201823566307789	500782294
2018	0001M18045E20189A1000P7M	519806935
2018	00000086354427PH4			705001569
2018	0000R111TXE05510PH2			519102044
2018	0002180214E01446P42			710079065
2018	0000747995365I7R			522705658
2018	0000696930639P7H			515859908

select * from data_warehouse.claim_detail_1_prt_mdcd where claim_id_src = '100020030201817948309523';


select * from dev.xz_temp5 where pcn like '%617028738%'; --not in here??
--aaaah it's a trim issue

select * from dev.xz_temp1 where member_id_src not in (select trim(pcn) from dev.xz_temp5);
2018	100020030201802601181231	705539101
2018	100020030201734861229682	516657665
2018	100023030201815634331942	516185760
2018	100023030201736273770044	503494932
2018	100020030201821960570947	526355010
2018	100020010201823967925753	527327932
2018	100020030201817245567223	528080343
2018	100020030201733850801792	608758727
2018	100020030201806035289095	618180134
2018	100020030201803206731173	719049578

select * from data_warehouse.claim_detail_1_prt_mdcd where claim_id_src = '100020030201802601181231';

select * from medicaid.clm_detail where icn = '100020030201802601181231';
select * from medicaid.htw_clm_detail where icn = '100020030201802601181231';

--dangit, i didn't include HTW
select * from dev.xz_temp1 where member_id_src not in (select trim(pcn) from dev.xz_temp5);
--none???!

select count(distinct member_id_src) from dev.xz_temp1;
--2963309
--2973065 --spc has more???








