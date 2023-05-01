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






















