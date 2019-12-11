--Activity
select *
from pg_stat_activity;

select pg_terminate_backend(65968);

select dbo.pg_kill_connection(119596)

select *
from pg_stat_ssl;

SELECT version();

--Total DB Size
select n.nspname, sum(relpages::bigint*8*1024) AS size
FROM pg_class
   JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
   WHERE relpages >= 8
   and n.nspname in ('dev')
   group by 1;
   
  SELECT pg_size_pretty( pg_total_relation_size('truven'||'.'||'ccaeo') );
 
--Size by Table
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
   and n.nspname in ('data_warehouse')
   ORDER BY 7 desc;
   
--Greenplum Distribution of a table
SELECT get_ao_distribution('data_warehouse.claim_detail_v1');

--Server Settings
SELECT *
FROM   pg_settings
WHERE  name like '%wal%';