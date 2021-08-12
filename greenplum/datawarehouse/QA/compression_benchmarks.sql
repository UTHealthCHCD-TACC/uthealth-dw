create table dev.truven_ccaet_zlib5
WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
as
select *
from truven.ccaet
distributed randomly;

--Analyze Both
analyze truven.ccaet; -- 36 secs
analyze dev.truven_ccaet_zlib5; -- 15 secs

--1: 
--4 secs
select count(*)
from truven.ccaet;

--4.5 secs
select count(*)
from dev.truven_ccaet_zlib5;

--2:

--7.2 secs
select plantyp, count(*) as cnt
from truven.ccaet
where year >= 2015 and year <= 2017
group by 1
order by 2 desc;

--6.8 secs
select plantyp, count(*) as cnt
from dev.truven_ccaet_zlib5
where year >= 2015 and year <= 2017
group by 1
order by 2 desc;

/*
 * Sizes
 */
select
   n.nspname,
   relname,
   reloptions,
   relacl,
   reltuples AS "#entries", 
   pg_size_pretty(relpages::bigint*8*1024) AS size_old,
   pg_total_relation_size(n.nspname||'.'||relname) as size_int,
   pg_size_pretty( pg_total_relation_size(n.nspname||'.'||relname)) as size_new
   FROM pg_class
   JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
   WHERE relpages >= 0
   and relname in ('ccaet', 'truven_ccaet_zlib5')
   ORDER BY 7 desc;

  select count(*) from dev2016.truv
