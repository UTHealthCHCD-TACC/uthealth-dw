-- qa_reporting.

drop table if exists dev.ip_truven_counts;
create table dev.ip_truven_counts
(year int,
table_name text,
row_count int,
pat_count int,
clm_count int,
reported_row_count int,
row_count_difference int,
row_percent_difference float,
last_updated date);

-- A - enrollment tables

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaea', count(*), count(distinct enrolid), 0
  from truven.ccaea
-- where year between 2019 and 2021
group by year;
 
insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcra', count(*), count(distinct enrolid), 0
  from truven.mdcra
-- where year between 2019 and 2021
group by year;

-- F - facility header tables

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaef', count(*), count(distinct enrolid), count(distinct msclmid)
  from truven.ccaef
-- where year between 2019 and 2021
group by year;
 
insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrf', count(*), count(distinct enrolid), count(distinct msclmid)
  from truven.mdcrf
-- where year between 2019 and 2021
group by year;

-- I - inpatient admission tables
-- possibly do enrolid::text || admdate::text for clmid
insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaei', count(*), count(distinct enrolid), count(distinct enrolid::text || admdate::text)
  from truven.ccaei
-- where year between 2019 and 2021
group by year;

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcri', count(*), count(distinct enrolid), count(distinct enrolid::text || admdate::text)
  from truven.mdcri
-- where year between 2019 and 2021
group by year;

-- O - outpatient tables

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaeo', count(*), count(distinct enrolid), count(distinct msclmid)
  from truven.ccaeo
-- where year between 2019 and 2021
group by year;

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcro', count(*), count(distinct enrolid), count(distinct msclmid)
  from truven.mdcro
-- where year between 2019 and 2021
group by year;

-- S - Inpatient Services table

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaes', count(*), count(distinct enrolid), count(distinct msclmid)
  from truven.ccaes
-- where year between 2019 and 2021
group by year;

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrs', count(*), count(distinct enrolid), count(distinct msclmid)
  from truven.mdcrs
-- where year between 2019 and 2021
group by year;

-- T - Detailed Enrollment table

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaet', count(*), count(distinct enrolid), 0
  from truven.ccaet
 where year between 2019 and 2021
group by year;
 
insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrt', count(*), count(distinct enrolid), 0
  from truven.mdcrt
-- where year between 2019 and 2021
group by year;

-- D - RX

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaed', count(*), count(distinct enrolid), count(distinct enrolid::text || ndcnum::text || svcdate::text)
  from truven.ccaed
-- where year between 2019 and 2021
group by year;

insert into dev.ip_truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrd', count(*), count(distinct enrolid), count(distinct enrolid::text || ndcnum::text || svcdate::text)
  from truven.mdcrd
-- where year between 2019 and 2021
group by year;

-- P ?

insert into dev.ip_truven_counts
(year, table_name, row_count)
select year, 'ccaep', count(*)
  from truven.ccaep
-- where year between 2019 and 2021
group by year;

insert into dev.ip_truven_counts
(year, table_name, row_count)
select year, 'mdcrp', count(*)
  from truven.mdcrp
-- where year between 2019 and 2021
group by year;



select * from dev.ip_truven_counts order by 1,2;

--update dev.ip_truven_counts set row_count_difference = row_count - reported_row_count;
--update dev.ip_truven_counts set row_percent_difference = 100. * abs(row_count - reported_row_count) / reported_row_count;
--update dev.ip_truven_counts set last_updated = now();