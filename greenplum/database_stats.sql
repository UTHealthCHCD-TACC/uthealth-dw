--Activity
select *
from pg_stat_activity;

select pg_terminate_backend(94497);

select dbo.pg_kill_connection(119596)

select *
from pg_stat_ssl;

SELECT version();

--Total DB Size
select SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT 
 FROM pg_tables;

--Total Schema Size
 SELECT schemaname, 
 SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT 
 FROM pg_tables 
 --WHERE schemaname in ('dw_qa', 'data_warehouse', 'dev', 'truven')
 group by 1
order by 2 desc;

--Size by Table
select
   n.nspname,
   --u.usename,
   relname,
   reloptions,
  -- relacl,
   reltuples AS "#entries",
   pg_size_pretty( pg_total_relation_size(n.nspname||'.'||relname)) as size_new
   FROM pg_class
   JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
   join pg_catalog.pg_user u on relowner=u.usesysid 
   WHERE relpages >= 0
   and n.nspname in ('dw_qa', 'data_warehouse')
   ORDER BY 3, 6 desc;
 

--Greenplum Distribution of a table
SELECT get_ao_distribution('dw_qa.claim_detail_diag');

select uth_member_id, count(*)
from dw_qa.dim_uth_claim_id
group by 1
order by 2 desc
limit 10;

select uth_member_id, count(*)
from dw_qa.claim_detail_diag cdd 
group by 1
order by 2 desc;

--Server Settings
SELECT *
FROM   pg_settings
WHERE  name like '%log%'; or name like'gp_%';

set gp_workfile_compress_algorithm to 'zlib';

-- Missing Statistics
SELECT * FROM gp_toolkit.gp_stats_missing;

-- Dead Space
SELECT * FROM gp_toolkit.gp_bloat_diag;

--Distribution Keys

SELECT
	pgn.nspname as table_owner
	,pgc.relname as table_name
	,COALESCE(pga.attname,'DISTRIBUTED RANDOMLY') as distribution_keys
from pg_catalog.gp_distribution_policy dp
JOIN pg_class AS pgc ON dp.localoid = pgc.oid
JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
LEFT OUTER JOIN pg_attribute pga ON dp.localoid = pga.attrelid and (pga.attnum = dp.distkey[0] or pga.attnum = dp.distkey[1] or pga.attnum = dp.distkey[2])
where pgn.nspname in ('dw_qa')
ORDER BY pgn.nspname, pgc.relname;

--Roles and Members
SELECT t.rarolename as RoleName ,t.ramembername as RoleMember
FROM pg_roles pr,
     (
      SELECT gra.rarolename, gra.ramembername
      FROM pg_roles spr, gp_toolkit.gp_roles_assigned gra
WHERE  gra.rarolename = spr.rolname
AND  spr.rolcanlogin = 'f'
) as t
WHERE pr.rolcanlogin =  'f'
AND pr.rolname = t.rarolename
ORDER BY t.rarolename, t.ramembername;


--Index Usage
SELECT   t.schemaname AS schema_name, t.tablename AS table_name    
,indexname AS index_name, c.reltuples AS num_rows
,pg_size_pretty(pg_relation_size(t.schemaname || '.' || t.tablename)) AS table_size
    ,pg_size_pretty(pg_relation_size(t.schemaname || '.' || indexrelname::text)) AS index_size
    ,CASE WHEN indisunique THEN 'Y'
       ELSE 'N'
    END AS UNIQUE
    ,idx_scan AS number_of_scans, idx_tup_read AS tuples_read
    ,idx_tup_fetch AS tuples_fetched
FROM pg_tables t
LEFT OUTER JOIN pg_class c ON t.tablename=c.relname
LEFT OUTER JOIN
( SELECT c.relname AS ctablename, ipg.relname AS indexname, x.indnatts AS number_of_columns, idx_scan, idx_tup_read, idx_tup_fetch, indexrelname, indisunique 
FROM pg_index x
   JOIN pg_class c ON c.oid = x.indrelid
   JOIN pg_class ipg ON ipg.oid = x.indexrelid
   JOIN pg_stat_all_indexes psai ON x.indexrelid = psai.indexrelid )
AS t1
ON t.tablename = t1.ctablename 
where t.schemaname in ('data_warhouse', 'dw_qa', 'dev', 'truven')
ORDER BY 1;

select pg_relation_size('dw_qa.claim_detail');

--Vacuum Analyze Status
SELECT pn.nspname
              ,pc.relname
              ,pslo.staactionname
              ,pslo.stasubtype
              ,pslo.statime as action_date
FROM pg_stat_last_operation pslo
RIGHT OUTER JOIN pg_class pc
ON pc.oid = pslo.objid 
AND pslo.staactionname 
IN ('VACUUM','ANALYZE')
INNER JOIN pg_namespace pn
ON pn.oid = pc.relnamespace
WHERE pc.relkind IN ('r','s')
AND pc.relstorage IN ('h', 'a', 'c')
and nspname in ('dw_qa')
order by 1, 2, 3;

analyze dw_qa.claim_detail;


