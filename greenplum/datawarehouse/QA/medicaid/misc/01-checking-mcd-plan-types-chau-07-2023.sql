/*************
 * check some plan types - as example, in fy2021, star health has 200 extra people out of 40,000 than dw. Why?
 */

select * from dev.xz_dwqa_temp1;

select mco_program_nm, count(*) from dev.xz_dwqa_temp1 where smib = '0' group by 1 order by 1;
CHIP					279145
FFS                 	208002
STAR                	4023922
STAR Health         	46062
STAR Kids           	174581
STAR+PLUS           	243891

select plan_type, count(*) from data_warehouse.member_enrollment_fiscal_yearly_1_prt_mdcd
where dual = '0' and fiscal_year = '2021' group by 1 order by 1;
CHIP		279122
FFS			208014
STAR		4023832
STAR Health	46247
STAR Kids	174478
STAR+PLUS	243884
--dw has more star health

select a.client_nbr as spc_memberid, b.member_id_src as dw_memberid
from dev.xz_dwqa_temp1 a full outer join data_warehouse.member_enrollment_fiscal_yearly_1_prt_mdcd b
on a.enrl_fy = b.fiscal_year and a.client_nbr = b.member_id_src
where a.mco_program_nm = 'STAR Health';

select mco_program_nm, count(distinct client_nbr) from dev.xz_dwqa_temp1 where smib = '0' group by 1 order by 1;
CHIP					279145
FFS                 	208002
STAR                	4023922
STAR Health         	46062
STAR Kids           	174581
STAR+PLUS           	243891

select plan_type, count(distinct member_id_src) from data_warehouse.member_enrollment_fiscal_yearly_1_prt_mdcd
where dual = '0' and fiscal_year = '2021' group by 1 order by 1;
CHIP			279122
FFS				208014
STAR			4023832
STAR Health		46247
STAR Kids		174478
STAR+PLUS		243884

drop table if exists dev.xz_discrepancies;

create table dev.xz_discrepancies as
with a as (select client_nbr from dev.xz_dwqa_temp1 where mco_program_nm like 'STAR Health%' and smib = '0'), 
b as (select member_id_src from data_warehouse.member_enrollment_fiscal_yearly_1_prt_mdcd
where plan_type = 'STAR Health' and dual = '0' and fiscal_year = '2021')
select a.client_nbr, b.member_id_src
from a full outer join b on a.client_nbr = b.member_id_src
where a.client_nbr is null or b.member_id_src is null;

--exists in dw not in spc
select * from dev.xz_discrepancies where client_nbr is null;
612714891
613828707
530443660
524956091
705457319

--exist in spc not dw
select * from dev.xz_discrepancies where member_id_src is null;
724747081
749843516
750006389
719929187
750006221

--how many?
select sum(case when client_nbr is null then 1 else 0 end) as spc_missing,
	sum(case when member_id_src is null then 1 else 0 end) as dw_missing
from dev.xz_discrepancies;
534	349




/************case 1: 612714891**********/

select member_id_src, plan_type, dual, total_enrolled_months 
from data_warehouse.member_enrollment_fiscal_yearly 
where fiscal_year = '2021' and member_id_src = '612714891';
519276404	STAR Health	0	12

select a.elig_date, a.contract_id, b.mco_program_nm
from medicaid.enrl a left join reference_tables.medicaid_lu_contract b
on a.contract_id = b.plan_cd
where client_nbr = '612714891' and year_fy = 2021;
--supposed to be in star health, 7 months in star health and 5 months in star
202103	K4	STAR Kids
202102	K4	STAR Kids
202101	K4	STAR Kids
202012	K4	STAR Kids
202011	K4	STAR Kids
202010	K4	STAR Kids
202009	K4	STAR Kids
202108	1E	STAR Health
202107	1E	STAR Health
202106	1E	STAR Health
202105	1E	STAR Health
202104	1E	STAR Health
202103	1E	STAR Health

/*****spc side code*********/
select CLIENT_NBR, MCO_PROGRAM_NM, AGE from medicaid.dbo.AGG_ENRL_MCD_FY where CLIENT_NBR = '612714891' and ENRL_FY = 2021;

select client_nbr, elig_date, CONTRACT_ID, SMIB from medicaid.dbo.ENRL_2021 where client_nbr = '612714891' order by 2;
--spc side agrees
612714891	202009	K4	0
612714891	202010	K4	0
612714891	202011	K4	0
612714891	202012	K4	0
612714891	202101	K4	0
612714891	202102	K4	0
612714891	202103	1E	0
612714891	202103	K4	0
612714891	202104	1E	0
612714891	202105	1E	0
612714891	202106	1E	0
612714891	202107	1E	0
612714891	202108	1E	0

/************case 2: 519488011***********/

select member_id_src, plan_type, dual, total_enrolled_months 
from data_warehouse.member_enrollment_fiscal_yearly 
where fiscal_year = '2021' and member_id_src = '519488011';
519488011	STAR Health	0	12

select a.client_nbr, a.elig_date, a.contract_id, b.mco_program_nm
from medicaid.enrl a left join reference_tables.medicaid_lu_contract b
on a.contract_id = b.plan_cd
where client_nbr = '519488011' and year_fy = 2021;
--supposed to be in star health, 7 months in star health and 5 months in star
519488011	202104	C2	STAR
519488011	202105	C2	STAR
519488011	202103	1E	STAR Health
519488011	202011	1E	STAR Health
519488011	202101	1E	STAR Health
519488011	202012	1E	STAR Health
519488011	202009	1E	STAR Health
519488011	202010	1E	STAR Health
519488011	202102	1E	STAR Health
519488011	202106	C3	STAR
519488011	202108	C3	STAR
519488011	202107	C3	STAR

/*****spc side code*********/
select client_nbr, elig_date, CONTRACT_ID from medicaid.dbo.ENRL_2021 where client_nbr = '519488011' order by 2;
--spc side agrees
519488011	202009	1E
519488011	202010	1E
519488011	202011	1E
519488011	202012	1E
519488011	202101	1E
519488011	202102	1E
519488011	202103	1E
519488011	202104	C2
519488011	202105	C2
519488011	202106	C3
519488011	202107	C3
519488011	202108	C3


/************case 3: 519531109***********/

select member_id_src, plan_type, dual, total_enrolled_months 
from data_warehouse.member_enrollment_fiscal_yearly 
where fiscal_year = '2021' and member_id_src = '519531109';
519531109	STAR Health	0	12

select a.client_nbr, a.elig_date, a.contract_id, b.mco_program_nm, a.smib
from medicaid.enrl a left join reference_tables.medicaid_lu_contract b
on a.contract_id = b.plan_cd
where client_nbr = '519531109' and year_fy = 2021;
--supposed to be in star health, 7 months in star health and 5 months in star
519531109	202108	1A	STAR	0
519531109	202107	1A	STAR	0
519531109	202106	1A	STAR	0
519531109	202105	1A	STAR	0
519531109	202104	1A	STAR	0
519531109	202103	1E	STAR Health	0
519531109	202102	1E	STAR Health	0
519531109	202101	1E	STAR Health	0
519531109	202012	1E	STAR Health	0
519531109	202011	1E	STAR Health	0
519531109	202010	1E	STAR Health	0
519531109	202009	1E	STAR Health	0

/*****spc side code*********/
select * from medicaid.dbo.AGG_ENRL_MCD_FY where CLIENT_NBR = '519531109' and ENRL_FY = 2021;
--he's star health???? wtf why wasn't he picked up?

select client_nbr, elig_date, CONTRACT_ID, SMIB from medicaid.dbo.ENRL_2021 where client_nbr = '519531109' order by 2;
--spc side agrees
519531109	202009	1E
519531109	202010	1E
519531109	202011	1E
519531109	202012	1E
519531109	202101	1E
519531109	202102	1E
519531109	202103	1E
519531109	202104	1A
519531109	202105	1A
519531109	202106	1A
519531109	202107	1A
519531109	202108	1A

--is it the lu contract?

select plan_cd, MCO_PROGRAM_NM from medicaid.dbo.LU_Contract where PLAN_CD = '1E';
--nope, that part tracks too
