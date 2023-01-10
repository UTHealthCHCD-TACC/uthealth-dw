
select * from dw_staging.member_enrollment_monthly_1_prt_truv;
select * from truven.ccaea;


select table_id_src, count(*) as rows, count(distinct member_id_src) as enrolids, count(distinct uth_member_id) as uth_member_ids
from dw_staging.member_enrollment_monthly_1_prt_truv
group by table_id_src;
--13200867984	141480684

select 'ccaea'::text as table_src, count(*) as rows, count(distinct enrolid) as enrolids from truven.ccaea;
--ccaea	382510264	134437437

select 'mdcra'::text as table_src, count(*) as rows, count(distinct enrolid) as enrolids from truven.mdcra;
--mdcra	28916421	9295969

select 'ccaet'::text as table_src, count(*) as rows, count(distinct enrolid) as enrolids from truven.ccaet;
--ccaet	3893195640	134437437

select 'mdcrt'::text as table_src, count(*) as rows, count(distinct enrolid) as enrolids from truven.mdcrt;

select * from 

select * from truven.ccaea
where enrolid::text not in (select member_id_src from dw_staging.member_enrollment_monthly_1_prt_truv)
limit 10;

select a.enrolid
from truven.ccaea a
left join

drop table if exists dev.xz_temp1;
drop table if exists dev.xz_temp2;
drop table if exists dev.xz_temp3;

create table dev.xz_temp1 as
select table_id_src, year, member_id_src || month_year_id as memid_yr_mth
from dw_staging.member_enrollment_monthly_1_prt_truv
distributed by (memid_yr_mth);

create table dev.xz_temp2 as
select 'ccaet'::text as table_id_src, year, enrolid || substring(replace(dtstart::varchar, '-', ''), 1, 6) as memid_yr_mth
from truven.ccaet
distributed by (memid_yr_mth);

create table dev.xz_temp3 as
select 'mdcrt'::text as table_id_src, year, enrolid || substring(replace(dtstart::varchar, '-', ''), 1, 6) as memid_yr_mth
from truven.mdcrt
distributed by (memid_yr_mth);

analyze dev.xz_temp1;
analyze dev.xz_temp2;
analyze dev.xz_temp3;

explain analyze
select table_id_src, year, count(distinct memid_yr_mth) as memid_yr_mths
  from dev.xz_temp2
  group by table_id_src, year;
 
 
explain
select table_id_src, count(distinct memid_yr_mth) as memid_yr_mths
  from dev.xz_temp3
  group by table_id_src;
 
explain
select table_id_src, year, count(distinct memid_yr_mth) as memid_yr_mths
  from dev.xz_temp3
  group by table_id_src, year;
 
 
 
 
select extract(day from dtstart) as day, count(*) from truven.ccaet
group by extract(day from dtstart)
order by extract(day from dtstart);

select distinct state from medicaid.chip_prov;



select primarytaxonomy from medicaid.chip_prov
where length(primarytaxonomy) = 2;

select file, length(primarytaxonomy) as primarytaxonomy_length, count(*) as count from medicaid.chip_prov
group by file, length(primarytaxonomy)
order by file, length(primarytaxonomy);

select file, length(npi) as npi_length, count(*) as count from medicaid.chip_prov
group by file, length(npi)
order by file, length(npi);

select npi from medicaid.chip_prov
where file like 'FY12%';

--TACC
select length(npi) as npi_length, count(*) as count from medicaid.chip_prov
where file like 'FY12%'
group by length(npi)
order by length(npi);

/*
npi_length	count
0			9036
1			394
2			984846
*/

--TACC
select distinct npi from medicaid.chip_prov
where file like 'FY12%'
and length(npi) = 2;

/*
12
A5
93
29
79
33
02
15
42
84
*/


/* check where those claims from 2021 went*/

drop table if exists dev.xz_dwqa_temp1;
drop table if exists dev.xz_dwqa_temp2;
drop table if exists dev.xz_dwqa_temp3;
drop table if exists dev.xz_dwqa_temp4;

--find msclmids from truven.ccaes for 2021
create table dev.xz_dwqa_temp1 as
select msclmid from truven.ccaes
where year = 2021
distributed by (msclmid);

--find claim_id_src from dw_staging.claim_detail for 2021
create table dev.xz_dwqa_temp2 as
select table_id_src, claim_id_src from dw_staging.claim_detail
where year = 2021
distributed by (claim_id_src);

--see if claims end up in >1 table
create table dev.xz_dwqa_temp2_2 as
select claim_id_src, count(distinct table_id_src) from dev.xz_dwqa_temp2
group by claim_id_src
distributed by (claim_id_src);

select count(*) from dev.xz_dwqa_temp2_2; --44628612

select count(*) from dev.xz_dwqa_temp2_2 --18847048
where count > 1;

--select 18847048/44628612.0; ...like 1/2 of them exist in >1 table

--ok let's drop this route and try enrolid | clmid combos

--find difference
create table dev.xz_dwqa_temp3 as
select a.msclmid, b.claim_id_src, b.table_id_src from dev.xz_dwqa_temp1 a
left join dev.xz_dwqa_temp2 b on a.msclmid = b.claim_id_src::bigint
distributed by (msclmid); --728

--grab rows from truven.ccaes that match those msclmids
create table dev.xz_dwqa_temp4 as 
select a.* from truven.ccaes a inner join dev.xz_dwqa_temp3 b
on a.msclmid = b.msclmid
where a.enrolid is not null
distributed by (msclmid);

select year, enrolid, msclmid, svcdate, tsvcdat from dev.xz_dwqa_temp4
limit 10;

/*
2021	4864412205	35487206	2021-12-20	2021-12-20
2021	5087733304	33375191	2021-10-29	2021-10-29
2021	2092057305	23043158	2021-11-12	2021-11-12
2021	4163972604	35590061	2021-12-27	2021-12-27
2020	4581281501	16741562	2020-11-18	2020-11-18
2020	4581281501	16741562	2020-11-18	2020-11-18
2020	4581281501	16741562	2020-11-18	2020-11-18
2020	4581281501	16741562	2020-11-18	2020-11-18
2021	4964901102	35561038	2021-12-17	2021-12-17
2021	4964901102	35561038	2021-12-17	2021-12-17 */

select enrolid, year from truven.ccaea
where enrolid = 2876829704; --exists in ccaea for 2017

select member_id_src, year from dw_staging.member_enrollment_yearly
where member_id_src = '2876829704'; --exists

select member_id_src, uth_member_id from data_warehouse.dim_uth_member_id
where member_id_src = '2876829704' -- exists

select member_id_src, claim_id_src, year from dw_staging.claim_detail
where member_id_src = '2876829704' and claim_id_src = '21391520'; --wtf it exists in here so why...

select member_id_src, claim_id_src, year from dw_staging.claim_detail
where member_id_src = '2876829704';

select msclmid, enrolid from staging_clean.ccaes_etl
where enrolid = '2876829704';

select count(*) from staging_clean.ccaes_etl;

select count(*) from staging_clean.ccaes_etl
where enrolid is null;

755666 / 558243566






/* check where those claims from 2021 went
 * TRY 2: ENROLID-CLMID COMBOS
 * */

drop table if exists dev.xz_dwqa_temp1;
drop table if exists dev.xz_dwqa_temp2;
drop table if exists dev.xz_dwqa_temp2_2;
drop table if exists dev.xz_dwqa_temp3;
drop table if exists dev.xz_dwqa_temp4;

--find enrolid || msclmids from truven.ccaes for 2021, exclude enrolid = nulls
create table dev.xz_dwqa_temp1 as
select enrolid::text || msclmid::text as concat from truven.ccaes
where year = 2021 and enrolid is not null
distributed by (concat);

--find claim_id_src from dw_staging.claim_detail for 2021 (nulls should be already excluded)
create table dev.xz_dwqa_temp2 as
select table_id_src, member_id_src || claim_id_src as concat from dw_staging.claim_detail
where year = 2021
distributed by (concat);

--see if claims end up in >1 table
create table dev.xz_dwqa_temp2_2 as
select concat, count(distinct table_id_src) from dev.xz_dwqa_temp2
group by concat
distributed by (concat);

select count(*) from dev.xz_dwqa_temp2_2; --273408550

select count(*) from dev.xz_dwqa_temp2_2 --183319
where count > 1;

--select 183319/273408550.0; 0.00067049475958231738 soooo there's some leakage, that might be the problem
--re-ran using enrolid = null stipulation in temp1, same results (weiiiird)

--try it again for a different year

drop table if exists dev.xz_dwqa_temp1;
drop table if exists dev.xz_dwqa_temp2;
drop table if exists dev.xz_dwqa_temp2_2;
drop table if exists dev.xz_dwqa_temp3;
drop table if exists dev.xz_dwqa_temp4;
drop table if exists dev.xz_dwqa_temp5;

--find enrolid || msclmids from truven.ccaes for 2021
create table dev.xz_dwqa_temp1 as
select enrolid::text || msclmid::text as concat from truven.ccaes
where year = 2020
distributed by (concat);

--find claim_id_src from dw_staging.claim_detail for 2021
create table dev.xz_dwqa_temp2 as
select table_id_src, member_id_src || claim_id_src as concat from dw_staging.claim_detail
where year = 2020
distributed by (concat);

--see if claims end up in >1 table
create table dev.xz_dwqa_temp2_2 as
select concat, count(distinct table_id_src) from dev.xz_dwqa_temp2
group by concat
distributed by (concat);

select count(*) from dev.xz_dwqa_temp2_2; --260306105

select count(*) from dev.xz_dwqa_temp2_2 --195308
where count > 1;

--so this problem exists in every year. So wtf is up with 2021

--ok whatevs, re-run script for 2021

--ok we're back to 2021 claims
--find difference
create table dev.xz_dwqa_temp3 as
select a.concat as truv_concat, b.concat as dw_concat, b.table_id_src from dev.xz_dwqa_temp1 a
left join dev.xz_dwqa_temp2 b on a.concat = b.concat
distributed by (truv_concat);

--see how many are missing
select count(*) from dev.xz_dwqa_temp3
where dw_concat is null; --63102

--grab rows from truven.ccaes that match those msclmids
create table dev.xz_dwqa_temp4 as 
select a.year, a.enrolid, a.msclmid, a.svcdate, a.tsvcdat from truven.ccaes a inner join dev.xz_dwqa_temp3 b
on a.enrolid:: text || a.msclmid::text = b.truv_concat
where a.enrolid is not null
and b.dw_concat is null
distributed by (msclmid);

--we have a lot of dupes, so get rid of those
create table dev.xz_dwqa_temp5 as
select distinct year, enrolid, msclmid, svcdate, tsvcdat from dev.xz_dwqa_temp4
distributed by (msclmid);

select * from dev.xz_dwqa_temp5
limit 10;

/*
year	enrolid		msclmid	svcdate		tsvcdat
2021	4713021102	2668913	2021-12-23	2021-12-23		--enrolid does not exist in ccaea
2021	4560214704	1482675	2021-12-17	2021-12-17		--enrolid does not exist in ccaea
2021	4560214704	1482675	2021-12-15	2021-12-15      --enrolid does not exist in mdcra
2021	4566607306	841681	2021-02-11	2021-02-11
2021	4520214403	35590950	2021-11-15	2021-11-15
2021	4219214204	13631772	2021-11-28	2021-11-28
2021	5090142207	7120591	2022-01-23	2022-01-23
2021	4562924704	1761078	2021-12-31	2021-12-31
2021	3643023102	6328388	2021-11-19	2021-11-19
2021	3024241502	451424	2021-06-21	2021-06-21 */

select member_id_src, uth_member_id from data_warehouse.dim_uth_member_id
where member_id_src = '4713021102' -- does not exist

select enrolid from truven.ccaea
where enrolid = 4566607306;

--looks like most of the problem is that there's an enrolid but it doesn't exist in ccaea

--let's check to see what percentage of the problem it is

create table dev.xz_dwqa_temp6 as
select a.enrolid as ccaes_enrolid, b.enrolid as ccaea_enrolid
from dev.xz_dwqa_temp5 a left join truven.ccaea b
on a.enrolid = b.enrolid
and b.year = 2021;

select * from dev.xz_dwqa_temp6;

select count(*) from dev.xz_dwqa_temp6; --25,052
--


create table dev.xz_dwqa_temp7 as
select a.enrolid as ccaes_enrolid, b.enrolid as ccaea_enrolid
from dev.xz_dwqa_temp5 a left join truven.ccaet b
on a.enrolid = b.enrolid
and b.year = 2021;

select * from dev.xz_dwqa_temp7;

select count(*) from dev.xz_dwqa_temp7; --25,052




























