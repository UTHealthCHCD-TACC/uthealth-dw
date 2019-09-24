--Activity
select *
from pg_stat_activity;

select pg_terminate_backend(22585);

select *
from pg_stat_ssl;

SELECT version();

--Total DB Size
select n.nspname, sum(relpages::bigint*8*1024) AS size
FROM pg_class
   JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
   WHERE relpages >= 8
   and n.nspname in ('data_warehouse')
   group by 1;
   
--Size by Table
select
   n.nspname,
   relname,
   reloptions,
   relacl,
   reltuples AS "#entries", pg_size_pretty(relpages::bigint*8*1024) AS size
   FROM pg_class
   JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
   WHERE relpages >= 8
   and n.nspname in ('data_warehouse')
   ORDER BY n.nspname, relpages desc;
   

  
  select * 
  from pg_namespace;