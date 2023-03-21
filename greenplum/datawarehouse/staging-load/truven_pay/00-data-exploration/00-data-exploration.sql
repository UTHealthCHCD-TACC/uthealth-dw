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




