/**********************************************
 * Data Warehouse - Truven - Data Exploration
 * 
 * Here we put all the questions about the data and answers so we don't run the same query over and over
 *********************************************/

/********************************************************
 * Question: Are there duplicated records in mdcrt
 * Answer: as of 3/21/2023 yes, but Xiaorui will fix it
 * Update: fixed!
 ********************************************************/
--after fixing
select year, count(*) from truven.mdcrt
group by year
order by year;

--after fixing	
year	count
2011	56639475
2012	51474037
2013	45238684
2014	41158747
2015	24246912
2016	23535300
2017	16283781
2018	12568958
2019	18237778
2020	18995409
2021	15439488

--before fixing
select year, count(*) from truven.mdcrt
group by year
order by year;

year	count
2011	113278950
2012	102948074
2013	90477368
2014	82317494
2015	24246912
2016	23535300
2017	16283781
2018	12568958
2019	18237778
2020	18995409
2021	15439488

select distinct enrolid from truven.mdcrt where year = 2011 limit 5;

28396623502
1904589001
626738002
68089801
1558913101

select * from truven.mdcrt where enrolid = 28396623502 order by dtstart;

enrolid		dtend		dtstart
28396623502	2011-01-31	2011-01-01
28396623502	2011-01-31	2011-01-01
28396623502	2011-02-28	2011-02-01
28396623502	2011-02-28	2011-02-01
28396623502	2011-03-31	2011-03-01
28396623502	2011-03-31	2011-03-01
28396623502	2011-04-30	2011-04-01
28396623502	2011-04-30	2011-04-01

--DUUUUPLICATES

select distinct * from truven.mdcrt where enrolid = 28396623502 order by dtstart;

/********************************************************
 * Question: how clean is Truven DOB and MSA?
 * DOB does not need cleaning
 ********************************************************/
select member_id_src, count(distinct dob_derived) from dw_staging.member_enrollment_monthly
where year = 2011
group by member_id_src
having count(distinct dob_derived) > 2;
--no records = squeaky clean

select member_id_src, count(distinct msa) from dw_staging.member_enrollment_monthly
where year = 2021
group by member_id_src
having count(distinct msa) > 2;
--many records with >1 distinct msa

/**********************************************************
 * Question: which years have zip3 and which years have msa?
 * Answer: 2011 - 2018 have zip 3 and 2019+ have msa
 ********************************************************/

select "year", count(zip3), count(msa) from dw_staging.member_enrollment_monthly_1_prt_truv
group by "year"
order by "year";

year	zip3		msa
2011	659520963	0
2012	656316613	0
2013	519240598	0
2014	543908294	0
2015	313350308	0
2016	319180004	0
2017	248724584	0
2018	256262881	0
2019	0	242901365
2020	0	229358691
2021	0	200319006


/**********************************
 * ETC
 *********************************/

--Look for active queries and kill if need be
select usename, pid, state, waiting, query_start , query, *
from pg_catalog.pg_stat_activity where
usename in ('xrzhang') and ---- put your username
state = 'active'
order by state, usename;

select usename, pid, state, waiting, query_start , query, *
from pg_catalog.pg_stat_activity where
usename in ('xrzhang') ---- put your username
order by state, usename;

select  pg_terminate_backend(272414); --- put in whatever the pid id is


/**********************************
 * Question: is ccaet ok?
 * Answer: appears so (3/21/23)
 *********************************/

select year, count(distinct enrolid) from truven.ccaet
group by year
order by year;

2011	55559154
2012	55975628
2013	43737217
2014	47258528
2015	28348363
2016	28717738
2017	26146275
2018	27087740
2019	25388778
2020	23306734
2021	22828135

select distinct enrolid from truven.ccaet where year = 2011 limit 5;

26611660202
28531033301
1253923303
725896004
28117809602

select * from truven.ccaet where enrolid = 28531033301 order by dtstart, dtend;

select count(*) from truven.ccaet where year = 2011; --564688998
select count(*) from truven.ccaet where year = 2011
group by enrolid, dtstart, dtend;








select year, count(*) from data_warehouse.member_enrollment_yearly_1_prt_truv
where total_enrolled_months is null
group by "year"
order by "year";



select year, total_enrolled_months from data_warehouse.member_enrollment_yearly_1_prt_truv
where total_enrolled_months is null
group by "year"
order by "year";

/************************************************
 * ISSUE: Truven has a lot of null MSAs in 2021/2022
 * 
 * My first assumption was that aggregating data on the yearly table
 * is why we have those nulls - it is true that if there is a null
 * for a month, then that member will be null for the whole year.
 * 
 * So I fixed the code and... rescued like, a tenth of a percent of records.
 * Two tenths of a percent if I'm being generous
 ************************************************/

--code to find the percent of missing zips and missing MSAs by year
select year, count(case when zip3 is null then 1 end) as null_zip_count,
	count(case when zip3 is not null then 1 end) as not_null_zip_count,
	count(case when zip3 is null then 1 end)/count(*)::float as zip_null_pct,
	count(case when msa is null then 1 end) as null_msa_count,
	count(case when msa is not null then 1 end) as not_null_msa_count,
	count(case when msa is null then 1 end)/count(*)::float as msa_null_pct
from dw_staging.truv_member_enrollment_yearly
group by year
order by year;

--BEFORE FIXING CODE
year	nullzip	notnullzip	nullzippct			nullmsa		notnullmsa	nullmsapct
2011	1771240	58690188	0.02929537158798168	60461428	0			1.0
2012	1324844	59191201	0.0218924419135454	60516045	0			1.0
2013	1266720	46459314	0.02654148886538529	47726034	0			1.0
2014	1183887	49681293	0.02327499873194197	50865180	0			1.0
2015	85053	30333486	0.0027960908970677	30418539	0			1.0
2016	91991	30633650	0.00299394893014599	0725641		0			1.0
2017	3618673	23911937	0.1314418024155658	27530610	0			1.0
2018	2989369	25159062	0.10620019993299093	28148431	0			1.0
2019	26942865	0		1.0					3080287		23862578	0.1143266315590417
2020	24929462	0		1.0					2848618		22080844	0.11426712698412826
2021	24150915	0		1.0					4362506		19788409	0.18063522645001234
2022	20831493	0		1.0					4017536	1	6813957		0.19285876437180954

--AFTER FIXING CODE
year	nullzip	notnullzip	nullzippct			nullmsa		notnullmsa	nullmsapct
2011	1645950	58815478	0.02722314133897069	60461428	0			1.0
2012	1223328	59292717	0.02021493638587915	60516045	0			1.0
2013	1098874	46627160	0.02302462425434302	47726034	0			1.0
2014	1117056	49748124	0.02196111367344026	50865180	0			1.0
2015	79199	30339340	0.00260364246948218	30418539	0			1.0
2016	88123	30637518	0.00286806058822336	30725641	0			1.0
2017	3543177	23987433	0.12869954570567088	27530610	0			1.0
2018	2942804	25205627	0.10454593366145346	28148431	0			1.0
2019	26942865	0		1.0					3029716		23913149	0.11244965967798896
2020	24929462	0		1.0					2776861		22152601	0.11138872551682022
2021	24150915	0		1.0					4309994		19840921	0.17846089889347877
2022	20831493	0		1.0	3				987121		16844372	0.1913987153969233

--...as you can see we changed the percent of missing nulls from 19.296% in to 19.140%...

/**************************
 * OK after talking to Lopita she wanted a few more deets:
 * commercial or medicare?
 * 
 * I also want to look at the raw tables as well
 **************************/

--code to find the percent of missing zips and missing MSAs by year (yearly table)
select table_id_src, year, count(case when zip3 is null then 1 end) as null_zip_count,
	count(case when zip3 is not null then 1 end) as not_null_zip_count,
	count(case when zip3 is null then 1 end)/count(*)::float as zip_null_pct,
	count(case when msa is null then 1 end) as null_msa_count,
	count(case when msa is not null then 1 end) as not_null_msa_count,
	count(case when msa is null then 1 end)/count(*)::float as msa_null_pct
from dw_staging.truv_member_enrollment_yearly
group by table_id_src, year
order by table_id_src, year;

--code to find the percent of missing zips and missing MSAs by year (monthly table)
select table_id_src, year, count(case when zip3 is null then 1 end) as null_zip_count,
	count(case when zip3 is not null then 1 end) as not_null_zip_count,
	count(case when zip3 is null then 1 end)/count(*)::float as zip_null_pct,
	count(case when msa is null then 1 end) as null_msa_count,
	count(case when msa is not null then 1 end) as not_null_msa_count,
	count(case when msa is null then 1 end)/count(*)::float as msa_null_pct
from dw_staging.truv_member_enrollment_monthly
group by table_id_src, year
order by table_id_src, year;

--code to find the percent of missing zips and missing MSAs by year (ccaea)
select 'ccaea' as table_id_src, year,
	count(case when empzip is null then 1 end) as null_zip_count,
	count(case when empzip is not null then 1 end) as not_null_zip_count,
	count(case when empzip is null then 1 end)/count(*)::float as zip_null_pct,
	count(case when msa is null then 1 end) as null_msa_count,
	count(case when msa is not null then 1 end) as not_null_msa_count,
	count(case when msa is null then 1 end)/count(*)::float as msa_null_pct
from truven.ccaea
group by "year"
order by year;

--code to find the percent of missing zips and missing MSAs by year (mdcra)
select 'mdcra' as table_id_src, year,
	count(case when empzip is null then 1 end) as null_zip_count,
	count(case when empzip is not null then 1 end) as not_null_zip_count,
	count(case when empzip is null then 1 end)/count(*)::float as zip_null_pct,
	count(case when msa is null then 1 end) as null_msa_count,
	count(case when msa is not null then 1 end) as not_null_msa_count,
	count(case when msa is null then 1 end)/count(*)::float as msa_null_pct
from truven.mdcra
group by "year"
order by year;

--code to find the percent of missing zips and missing MSAs by year (ccaet)
select 'ccaet' as table_id_src, year,
	count(case when empzip is null then 1 end) as null_zip_count,
	count(case when empzip is not null then 1 end) as not_null_zip_count,
	count(case when empzip is null then 1 end)/count(*)::float as zip_null_pct,
	count(case when msa is null then 1 end) as null_msa_count,
	count(case when msa is not null then 1 end) as not_null_msa_count,
	count(case when msa is null then 1 end)/count(*)::float as msa_null_pct
from truven.ccaet
group by "year"
order by year;

--code to find the percent of missing zips and missing MSAs by year (mdcrt)
select 'mdcrt' as table_id_src, year,
	count(case when empzip is null then 1 end) as null_zip_count,
	count(case when empzip is not null then 1 end) as not_null_zip_count,
	count(case when empzip is null then 1 end)/count(*)::float as zip_null_pct,
	count(case when msa is null then 1 end) as null_msa_count,
	count(case when msa is not null then 1 end) as not_null_msa_count,
	count(case when msa is null then 1 end)/count(*)::float as msa_null_pct
from truven.mdcrt
group by "year"
order by year;

--edited the code so that 0s also count as missing/null

--code to find the percent of missing zips and missing MSAs by year (ccaea)
select 'ccaea' as table_id_src, year,
	count(case when (empzip is null or empzip = 0) then 1 end) as null_zip_count,
	count(case when (empzip is not null and empzip != 0) then 1 end) as not_null_zip_count,
	count(case when (empzip is null or empzip = 0) then 1 end)/count(*)::float as zip_null_pct,
	count(case when (msa is null or msa = 0) then 1 end) as null_msa_count,
	count(case when (msa is not null and msa !=0) then 1 end) as not_null_msa_count,
	count(case when (msa is null or msa = 0) then 1 end)/count(*)::float as msa_null_pct
from truven.ccaea
group by "year"
order by year;

--code to find the percent of missing zips and missing MSAs by year (mdcra)
select 'mdcra' as table_id_src, year,
	count(case when (empzip is null or empzip = 0) then 1 end) as null_zip_count,
	count(case when (empzip is not null and empzip != 0) then 1 end) as not_null_zip_count,
	count(case when (empzip is null or empzip = 0) then 1 end)/count(*)::float as zip_null_pct,
	count(case when (msa is null or msa = 0) then 1 end) as null_msa_count,
	count(case when (msa is not null and msa !=0) then 1 end) as not_null_msa_count,
	count(case when (msa is null or msa = 0) then 1 end)/count(*)::float as msa_null_pct
from truven.mdcra
group by "year"
order by year;

/********************
 * Question: Do 's' tables have all the proc codes in 'i' tables
 * Ans: it would appear so
 *******************/

select enrolid, caseid, proc15 from truven.ccaei where proc15 is not null;
892732903	120871	J2543     
892929702	120886	93970     
1072301801	141070	82330     
1402473401	235054	99233     
1403514402	235111	C9113  

--assumption here being that if the 15th proc code is in the 's' tables, then proc codes 1-14 are also in the 's' table
select * from truven.ccaes where enrolid = '892732903' and caseid = '120871' and proc1 = 'J2543';

/***********************
 * Question: What's the distribution of code types in 'o' tables?
 * Ans: Varies by mdcro or ccaeo, but
 *		mdcro: about 70-20-10 CPT-HCPCS-NULL
 *		ccaeo: about 85-10-5  CPT-HCPCS-NULL
 *		both tables: <1% CDT
 **********************/

select proctyp, count(*) as count from truven.mdcro
where year = 2021 group by proctyp;

[null]	3919671
8    	1915
7    	16929583
1    	65094658

select proctyp, count(*) as count from truven.mdcro
where year = 2016 group by proctyp;
[null]	9588897
1    	85368231
8    	1344
7    	24023812

select proctyp, count(*) as count from truven.ccaeo
where year = 2021 group by proctyp;
[null]	22440193
1    	440417731
8    	58745
7    	71741267

select proctyp, count(*) as count from truven.ccaeo
where year = 2016 group by proctyp;
[null]	34291182
1    	527014549
7    	62196156
*    	1
8    	139130

/********************************
 * Question: What's the distribution of code types in 's' tables?
 * Ans:
 */

select proctyp, count(*) as count from truven.mdcrs
where year = 2016 group by proctyp;


select proctyp, count(*) as count from truven.mdcrs
where year = 2021 group by proctyp;


select proctyp, count(*) as count from truven.ccaes
where year = 2016 group by proctyp;


select proctyp, count(*) as count from truven.ccaes
where year = 2021 group by proctyp;

--of null, how many COULD BE ICD Codes?

select length(proc1), count(*) as count from truven.mdcrs
where year = 2016 and proc1 is not null and proctyp is null 
group by length(proc1);

select length(proc1), count(*) as count from truven.mdcrs
where year = 2021 and proc1 is not null and proctyp is null 
group by length(proc1);

select length(proc1), count(*) as count from truven.ccaes
where year = 2016 and proc1 is not null and proctyp is null 
group by length(proc1);

select length(proc1), count(*) as count from truven.ccaes
where year = 2021 and proc1 is not null and proctyp is null 
group by length(proc1);

--are all the null fields just where proc1 is missing

select count(*) from truven.mdcrs
where proc1 is not null and proctyp is null;

select count(*) from truven.ccaes
where proc1 is not null and proctyp is null;

/*************************
 * REGEXXXX
 ************************/

select proc1, proctyp from truven.mdcro
where proc1 ~ '^[a-zA-Z].{2,6}$'
and year = 2021 and proctyp is null;
--NADA

select proc1, proctyp from truven.mdcrs
where proc1 ~ '^[a-zA-Z].{2,6}$'
and year = 2021 and proctyp is null;


select case when '12345' ~ '^[a-zA-Z].{2,6}$' then 1 else 0 end as nums_only,
	case when 'A12345' ~ '^[a-zA-Z].{2,6}$' then 1 else 0 end as correct,
	case when 'A2'  ~ '^[a-zA-Z].{2,6}$' then 1 else 0 end as too_short,
	case when 'A1359312'  ~ '^[a-zA-Z].{2,6}$' then 1 else 0 end as too_long;

/*******************************
 * Is the Redbook strictly 1 row per ndc or what
 * Ans: yes it is
 ******************************/

select * from reference_tables.redbook;

select ndcnum, count(*) from reference_tables.redbook
group by ndcnum
having count(*) > 1;

/*******************************
 * oookay. what about staging_clean.truven_rx_claim_id
 *****/

select member_id_src, rx_claim_id_src, count(*) from staging_clean.truven_rx_claim_id
group by member_id_src, rx_claim_id_src
having count(*) > 1;

4392411401	43924114013781817772017-10-07	2
31368965401	313689654011722089602014-06-29	2
2028649001	2028649001647640175302013-10-28	2
1732598201	17325982011730682202016-09-21	2
4332160802	43321608021730696002016-08-29	2
33218918501	33218918501592671000012021-03-23	2
2773751102	2773751102683820016012015-09-28	2
1633871403	16338714031439887012021-06-19	2
25340432902	25340432902633040296012013-02-15	2
4380406701	4380406701678770198052021-06-01	2
4819524001	4819524001690970849052019-07-13	2
4819781303	4819781303932203052020-04-16	2

--how many rows?

select * from data_warehouse.dim_uth_rx_claim_id
where member_id_src = '4392411401' and rx_claim_id_src = '43924114013781817772017-10-07';

truv	2017	27610302443	43924114013781817772017-10-07	660815682	4392411401
truv	2017	27610302444	43924114013781817772017-10-07	660815682	4392411401
--well there's your problem, you're assigning two uth_rx_claim_ids for one rx_claim

--ok I rebuilt the dim_uth_rx_claim_id, how is it now
select * from data_warehouse.dim_uth_rx_claim_id
where member_id_src = '4392411401' and rx_claim_id_src = '43924114013781817772017-10-07';
--only one row!

select member_id_src, rx_claim_id_src, count(*) from staging_clean.truven_rx_claim_id
group by member_id_src, rx_claim_id_src
having count(*) > 1;
--NONE! OK at least one problem has been fixed. So wtf is going on with the other thing.

vacuum analyze dw_staging.truv_pharmacy_claims;

select count(*) from dw_staging.truv_pharmacy_claims where table_id_src = 'ccaed';


select distinct




