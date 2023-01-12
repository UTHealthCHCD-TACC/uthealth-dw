
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

/* new problem:
 * work out counting method for claim-diag and claim-icd-proc
 */

drop table if exists dev.xz_dwqa_temp;
drop table if exists dev.xz_dwqa_temp2;

create table dev.xz_dwqa_temp (
	year int,
	concat text
) distributed by (concat);

create table dev.xz_dwqa_temp2 as
select year, member_id_src || claim_id_src as concat from dw_staging.claim_diag
where year = 2011
distributed by (concat);

insert into dev.xz_dwqa_temp
select year, count(distinct concat) from dev.xz_dwqa_temp2
group by year;

select * from dev.xz_dwqa_temp;

/* See if pdx is ALWAYS filled in before dx1 */

select pdx, dx1 from truven.ccaes
where pdx is null 
limit 10;

--dangit, looks like sometimes the pdx is left null while dx1 has info

--ok so we've fixed the dx existing problem, but there are still enrolid || clm combos left over
--maybe it's missing enrolids?

drop table if exists dev.xz_dwqa_temp;

create table dev.xz_dwqa_temp as
select year, 'mdcrs'::text as table_src, enrolid, enrolid::text || msclmid::text as concat
from truven.mdcrs
where enrolid is not null
and (pdx is not null or
  dx1 is not null or
  dx2 is not null or
  dx3 is not null)
distributed by (concat);

select a.year, a.table_src, count(distinct a.concat)
from dev.xz_dwqa_temp a inner join truven.mdcra b 
on a.enrolid = b.enrolid and a.year = b.year
group by a.year, a.table_src
order by a.year;

/* problem: claim diag doesn't match, let's find out why 
 * Let's use 2018 as an example
 */

drop table if exists dev.xz_dwqa_temp1; --scrape all enrolid, clmids for 2018 from dw.claim_diag
drop table if exists dev.xz_dwqa_temp2; --get just the distinct ones
drop table if exists dev.xz_dwqa_temp3; --scrape all enrolid, clmids fro 2018 from truven tables
drop table if exists dev.xz_dwqa_temp4; --get distinct, enrolid exists in ccaea/mdcra tables only
drop table if exists dev.xz_dwqa_temp5; --this is the table of which ones are not included in the other one

--temp1
create table dev.xz_dwqa_temp1 as
select member_id_src as enrolid, claim_id_src as clmid, member_id_src || claim_id_src as concat
from dw_staging.claim_diag
where year = 2018
distributed by (concat);

--temp2
create table dev.xz_dwqa_temp2 as
select max(enrolid) as enrolid, max(clmid) as clmid, concat
from dev.xz_dwqa_temp1
group by concat
distributed by (concat);

--temp3
create table dev.xz_dwqa_temp3 as
select enrolid, msclmid, enrolid::text || msclmid::text as concat
from truven.ccaes
where year = 2018
and enrolid is not null
and (pdx is not null or
	dx1 is not null or
	dx2 is not null or 
	dx3 is not null or 
	dx4 is not null)
distributed by (concat);

insert into dev.xz_dwqa_temp3
select enrolid, msclmid, enrolid::text || msclmid::text as concat
from truven.mdcrs
where year = 2018
and enrolid is not null
and (pdx is not null or
	dx1 is not null or
	dx2 is not null or 
	dx3 is not null or 
	dx4 is not null);

insert into dev.xz_dwqa_temp3
select enrolid, msclmid, enrolid::text || msclmid::text as concat
from truven.ccaeo
where year = 2018
and enrolid is not null
and (dx1 is not null or
	dx2 is not null or 
	dx3 is not null or 
	dx4 is not null);

insert into dev.xz_dwqa_temp3
select enrolid, msclmid, enrolid::text || msclmid::text as concat
from truven.mdcro
where year = 2018
and enrolid is not null
and (dx1 is not null or
	dx2 is not null or 
	dx3 is not null or 
	dx4 is not null);

--temp4
with b as (select enrolid from truven.ccaea where year = 2018
	union
	select enrolid from truven.mdcra where year = 2018)

select distinct a.enrolid, a.msclmid, a.concat
into dev.xz_dwqa_temp4
from dev.xz_dwqa_temp3 a inner join b 
on a.enrolid = b.enrolid;

--temp5
create table dev.xz_dwqa_temp5 as
select a.enrolid as dw_enrolid, a.clmid as dw_clmid, a.concat as dw_concat,
b.enrolid as truv_enrolid, b.msclmid as truv_clmid, b.concat as truv_concat
from dev.xz_dwqa_temp2 a full outer join dev.xz_dwqa_temp4 b on
a.concat = b.concat
distributed by (truv_concat);

--now the fun part
--select only the ones that are in dw but not truven
select * from dev.xz_dwqa_temp5 where truv_concat is null
limit 5;

/*enrolid	clmid	concat
1006199705	5237004	10061997055237004 --enrolid in 2017
1006951207	5020677	10069512075020677 --enrolid in 2016, 2017
1006951207	5020698	10069512075020698 --same as prev
1014050404	105387	1014050404105387  --enrolid in many years but not 2018
1040207704	2808928	10402077042808928 --enrolid in many years but not 2018
 */

select * from dev.xz_dwqa_temp5 where dw_concat is null 
limit 5;

/*enrolid	clmid	concat
1009214901	4649	10092149014649  --enrolid is in 2018, clm is in ccaeo, didn't match dw bc year = 2017
1017215802	58104	101721580258104 --enrolid is in 2018, 
1022158902	277227	1022158902277227
1026194001	28940	102619400128940
1045146101	3	10451461013
 */

select enrolid, year from truven.ccaea
where enrolid = 1017215802;

--case 1
/*enrolid	clmid	concat
1009214901	4649	10092149014649 */

select year, enrolid, msclmid, dx1, dx2, dx3, dx4 from truven.ccaeo
where enrolid = 1009214901 and msclmid = 4649;

/*enrolid	clmid	dx1		(dx2-4 are empty)
2018	1009214901	4649	G43009    			
2018	1009214901	4649	G43009    			
2018	1009214901	4649	G43009    			
2017	1009214901	4649	M545      					
*/

select year, member_id_src, claim_id_src, diag_cd from dw_staging.claim_diag
where member_id_src = '1009214901' and claim_id_src = '4649';
/*yr	enrolid		clmid	dx
2017	1009214901	4649	G43009
*/

--case 2
/*enrolid	clmid	concat
1017215802	58104	101721580258104 */

select year, svcdate, enrolid, msclmid, dx1, dx2, dx3, dx4 from truven.ccaeo
where enrolid = 1017215802 and msclmid = 58104;

/*enrolid	clmid	dx1		dx2			dx3			dx4
2018	1017215802	58104	C3490     			
2018	1017215802	58104	C3490     			
2017	1017215802	58104	S43432A   	M7502     	M75112    */

select year, from_date_of_service, member_id_src, claim_id_src, diag_cd from dw_staging.claim_diag
where member_id_src = '1017215802' and claim_id_src = '58104';

/*yr	enrolid		clmid	dx
2017	1017215802	58104	C3490
2017	1017215802	58104	M7502
2017	1017215802	58104	M75112 */

--are these claims that span years?
select * from truven.ccaeo
where enrolid = 1017215802 and msclmid = 58104;

--answer: no

--are shorter claim ids more likely to be problematic?

select length(truv_clmid::text) as msclmid_length, count(*) as count
from dev.xz_dwqa_temp5 
where dw_concat is null
group by length(truv_clmid::text)
order by length(truv_clmid::text);

select length(truv_clmid::text) as msclmid_length, count(*) as count
from dev.xz_dwqa_temp5 
group by length(truv_clmid::text)
order by length(truv_clmid::text);

--answer: yes

--what percentage of claims have same enrolid, same msclmid, but appear to be totally different claims?
drop table if exists dev.xz_dwqa_temp7;

create table dev.xz_dwqa_temp7 as
select a.enrolid, a.msclmid, count(distinct a.year) as years
from truven.ccaeo a inner join dev.xz_dwqa_temp5 b
on a.enrolid = b.truv_enrolid and a.msclmid = b.truv_clmid
group by a.enrolid, a.msclmid
distributed by (msclmid);

select count(*) from dev.xz_dwqa_temp7;
--250027063 rows

select count(*) from dev.xz_dwqa_temp7
where years > 1;
--92964

select 92964/250027063.0; --0.00037181575020140920

--okay so it's fairly negligible

/*testing to get count of distinct enrolid || msclmids with procs*/
drop table if exists dev.xz_dwqa_temp;

create table dev.xz_dwqa_temp as
select year, 'mdcrs'::text as table_src, enrolid::text || msclmid::text as concat
from truven.mdcrs
where enrolid is not null
and pproc is not null
distributed by (concat);

insert into dev.xz_dwqa_temp
select year, 'mdcrf'::text as table_src, enrolid::text || msclmid::text as concat
from truven.mdcrf
where (proc1 is not null or
  proc2 is not null or
  proc3 is not null or
  proc4 is not null or
  proc5 is not null or
  proc6 is not null);

/*UGGGGGGGGGGGGGHHHH*/
 /*Now I have to find out why the numbers aren't matching up for icd proc*/

/* problem: claim diag doesn't match, let's find out why 
 * Let's use 2018 as an example
 */

drop table if exists dev.xz_dwqa_temp1; --scrape all enrolid, clmids for 2018 from dw.claim_diag
drop table if exists dev.xz_dwqa_temp2; --get just the distinct ones
drop table if exists dev.xz_dwqa_temp3; --scrape all enrolid, clmids fro 2018 from truven tables
drop table if exists dev.xz_dwqa_temp4; --get distinct, enrolid exists in ccaea/mdcra tables only
drop table if exists dev.xz_dwqa_temp5; --this is the table of which ones are not included in the other one

--temp1
create table dev.xz_dwqa_temp1 as
select member_id_src as enrolid, claim_id_src as clmid, member_id_src || claim_id_src as concat
from dw_staging.claim_icd_proc
where year = 2018
distributed by (concat);

--temp2
create table dev.xz_dwqa_temp2 as
select max(enrolid) as enrolid, max(clmid) as clmid, concat
from dev.xz_dwqa_temp1
group by concat
distributed by (concat);

--temp3
create table dev.xz_dwqa_temp3 as
select enrolid, msclmid, enrolid::text || msclmid::text as concat
from truven.ccaes
where year = 2018
and enrolid is not null
and pproc is not null
distributed by (concat);

insert into dev.xz_dwqa_temp3
select enrolid, msclmid, enrolid::text || msclmid::text as concat
from truven.mdcrs
where year = 2018
and enrolid is not null
and pproc is not null;

insert into dev.xz_dwqa_temp3
select enrolid, msclmid, enrolid::text || msclmid::text as concat
from truven.ccaef
where year = 2018
and enrolid is not null
and (proc1 is not null or
	proc2 is not null or 
	proc3 is not null or 
	proc4 is not null or 
	proc5 is not null or 
	proc6 is not null);

insert into dev.xz_dwqa_temp3
select enrolid, msclmid, enrolid::text || msclmid::text as concat
from truven.mdcrf
where year = 2018
and enrolid is not null
and (proc1 is not null or
	proc2 is not null or 
	proc3 is not null or 
	proc4 is not null or 
	proc5 is not null or 
	proc6 is not null);

--temp4
select distinct *
into dev.xz_dwqa_temp4
from dev.xz_dwqa_temp3;

--temp5
create table dev.xz_dwqa_temp5 as
select a.enrolid as dw_enrolid, a.clmid as dw_clmid, a.concat as dw_concat,
b.enrolid as truv_enrolid, b.msclmid as truv_clmid, b.concat as truv_concat
from dev.xz_dwqa_temp2 a full outer join dev.xz_dwqa_temp4 b on
a.concat = b.concat
distributed by (truv_concat);

select count(*) from dev.xz_dwqa_temp2; --9797024 distinct from dw
select count(*) from dev.xz_dwqa_temp4; --9788278 distinct from truven

--now the fun part
--select only the ones that are in dw but not truven
select * from dev.xz_dwqa_temp5 where truv_concat is null
limit 5;

/*enrolid	clmid	concat
1011347702	29637	101134770229637
1011380202	26672	101138020226672
1011388401	26630	101138840126630
1011428801	27336	101142880127336
1011468202	55824	101146820255824
 */

select * from dev.xz_dwqa_temp5 where dw_concat is null 
limit 5;

--none?

select enrolid, year from truven.ccaea
where enrolid = 1017215802;

--case 1
/*enrolid	clmid	concat
1009214901	4649	10092149014649 */

select year, enrolid, msclmid, dx1, dx2, dx3, dx4 from truven.ccaeo
where enrolid = 1009214901 and msclmid = 4649;

/*enrolid	clmid	dx1		(dx2-4 are empty)
2018	1009214901	4649	G43009    			
2018	1009214901	4649	G43009    			
2018	1009214901	4649	G43009    			
2017	1009214901	4649	M545      					
*/

select year, member_id_src, claim_id_src, diag_cd from dw_staging.claim_diag
where member_id_src = '1009214901' and claim_id_src = '4649';
/*yr	enrolid		clmid	dx
2017	1009214901	4649	G43009
*/

--case 2
/*enrolid	clmid	concat
1017215802	58104	101721580258104 */

select year, svcdate, enrolid, msclmid, dx1, dx2, dx3, dx4 from truven.ccaeo
where enrolid = 1017215802 and msclmid = 58104;

/* working on rx table now*/

select * from dw_staging.pharmacy_claims where ndc is null limit 10;
--none

select * from truven.ccaed where ndcnum is null limit 10;
--none

--ok so if we only take rows where enrolid exists, then that's good enough

drop table if exists dev.xz_dwqa_temp;

create table dev.xz_dwqa_temp as
select year, 'mdcrd'::text as table_src,
  enrolid::text || ndcnum::text || svcdate::text as concat
from truven.mdcrd
where enrolid is not null
distributed by (concat);

drop table if exists dev.xz_dwqa_temp;

create table dev.xz_dwqa_temp as
  select year, table_id_src, rx_claim_id_src as rx_id from dw_staging.pharmacy_claims
  where table_id_src = 'mdcrd'
  distributed by (rx_id);

select year, table_id_src, count(distinct rx_id) from dev.xz_dwqa_temp
  group by year, table_id_src;

--problem: no rx claims in 2021 for ccaed
 
 select year, enrolid, ndcnum, svcdate from truven.ccaed
 where year = 2021 and enrolid is not null
limit 5;
--entries exist

select year, member_id_src, ndc, fill_date from dw_staging.pharmacy_claims
where year = 2021 and table_id_src = 'ccaed';
--a lot of null member_id_srcs here

select year, member_id_src, ndc, fill_date from dw_staging.pharmacy_claims
where year = 2021 and table_id_src = 'ccaed'
and member_id_src is not null;
--all blank?????

--how many enrolids are null in rx table for ccae

select count(*) from truven.ccaed
where year = 2021; --170987410

select count(*) from truven.ccaed
where year = 2021 and enrolid is not null; --170869240

select 170987410 - 170869240; -- 118170
select 118170 / 170987410.0; --0.00069110351458040098

--check the ETL

select count(*) from staging_clean.ccaed_etl
where year = 2021; --170987410

select count(*) from staging_clean.ccaed_etl
where year = 2021 and enrolid is not null; --170869240

--get sample of enrolids;
select distinct enrolid from truven.ccaed
where year = 2021 and enrolid is not null
limit 10;

/*4931698203
4009402802
4143395502
4553427502
34173329501
33581613501
32693719403
4775788803
3066781103
2827054703*/

select member_id_src from data_warehouse.dim_uth_member_id
where member_id_src = '4931698203'; --exists in dim

select member_id_src from data_warehouse.dim_uth_member_id
where member_id_src = '4009402802'; --exists in dim

--nm Joe found the error, it was a case of a.member_id_src vs a.enrolid on the etl




















