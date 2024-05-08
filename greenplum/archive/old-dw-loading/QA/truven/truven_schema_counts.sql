-- backup old counts just in case
drop table if exists qa_reporting.truven_counts_old;

SELECT *
INTO qa_reporting.truven_counts_old
from qa_reporting.truven_counts;

drop table if exists qa_reporting.truven_counts;
create table qa_reporting.truven_counts
(year int,
table_name text,
row_count bigint,
pat_count bigint,
clm_count bigint,
reported_row_count bigint,
row_count_difference bigint,
row_percent_difference float,
last_updated date);

-- A - enrollment tables

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaea', count(*), count(distinct enrolid), 0
  from truven.ccaea
-- where year between 2019 and 2021
group by year;
 
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcra', count(*), count(distinct enrolid), 0
  from truven.mdcra
-- where year between 2019 and 2021
group by year;

-- F - facility header tables

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaef', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.ccaef
-- where year between 2019 and 2021
group by year;
 
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrf', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.mdcrf
-- where year between 2019 and 2021
group by year;

-- I - inpatient admission tables
-- possibly do enrolid::text || admdate::text for clmid
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaei', count(*), count(distinct enrolid), count(distinct enrolid::text || admdate::text)
  from truven.ccaei
-- where year between 2019 and 2021
group by year;

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcri', count(*), count(distinct enrolid), count(distinct enrolid::text || admdate::text)
  from truven.mdcri
-- where year between 2019 and 2021
group by year;

-- O - outpatient tables

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaeo', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.ccaeo
-- where year between 2019 and 2021
group by year;

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcro', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.mdcro
-- where year between 2019 and 2021
group by year;

-- S - Inpatient Services table

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaes', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.ccaes
-- where year between 2019 and 2021
group by year;

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrs', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.mdcrs
-- where year between 2019 and 2021
group by year;

-- T - Detailed Enrollment table

--delete from qa_reporting.truven_counts where table_name=  'ccaeo' and last_updated is null;

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaet', count(*), count(distinct enrolid), 0
  from truven.ccaet
-- where year between 2019 and 2021
group by year;
 		
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrt', count(*), count(distinct enrolid), 0
  from truven.mdcrt
-- where year between 2019 and 2021
group by year;

-- D - RX

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaed', count(*), count(distinct enrolid), count(distinct enrolid::text || ndcnum::text || svcdate::text)
  from truven.ccaed
-- where year between 2019 and 2021
group by year;

insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrd', count(*), count(distinct enrolid), count(distinct enrolid::text || ndcnum::text || svcdate::text)
  from truven.mdcrd
-- where year between 2019 and 2021
group by year;

-- P ?

insert into qa_reporting.truven_counts
(year, table_name, row_count)
select year, 'ccaep', count(*)
  from truven.ccaep
-- where year between 2019 and 2021
group by year;

insert into qa_reporting.truven_counts
(year, table_name, row_count)
select year, 'mdcrp', count(*)
  from truven.mdcrp
-- where year between 2019 and 2021
group by year;

select count(distinct table_name) from qa_reporting.truven_counts;

select * from qa_reporting.truven_counts order by 1,2;