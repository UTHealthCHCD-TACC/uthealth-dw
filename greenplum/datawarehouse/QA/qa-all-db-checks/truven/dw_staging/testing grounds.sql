
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


