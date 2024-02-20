/* ******************************************************************************************************
 *  Collection of queries for exploring database settings and performance/resource usage
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wallingTACC  || 1/1/2019 || script created
 * ******************************************************************************************************
 */

--Activity
select *
from pg_stat_activity
where state='active'
and usename='gmunoz1';

select *
from session_state.session_level_memory_consumption

SELECT * FROM gp_toolkit.gp_bloat_diag;

select version();

--Permissions
SELECT 
  nspname,         -- schema name
  defaclobjtype,   -- object type
  defaclacl        -- default access privileges
FROM pg_default_acl a 
JOIN pg_namespace b ON a.defaclnamespace=b.oid
where nspname in ()

select *
from gp_toolkit.gp_skew_coefficients;

where skcrelname like 'wc%';

select pg_terminate_backend(276630);

--General Settings
select *
from pg_settings
where name like '%max%';

select ceil((200 + 3 + 15 + 5) / 16)

-- GIS
SELECT * 
FROM pg_extension;

show search_path;

ALTER DATABASE uthealth SET search_path=public,gis;

select dbo.pg_kill_connection(119596)

create extension postgis schema gis
CREATE EXTENSION postgis_tiger_geocoder schema gis;

create extension pgrouting schema gis 


select *
from pg_stat_ssl;

SELECT version();

--get activity timestamps on a db object
select * from pg_stat_operations where objname = 'claim_header';

--Total DB Size
select SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT
 FROM pg_tables;



select pg_size_pretty(pg_database_size('uthealth'));
 
--Total Schema Size
 SELECT schemaname,
 pg_size_pretty(SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::BIGINT) 
 FROM pg_tables 
-- WHERE schemaname in ('truven')
 group by 1
order by 2 desc;


--Size by Table
select
   n.nspname,
   u.usename,
   relname,
   reloptions,
   relacl,
   reltuples AS "#entries",
   pg_size_pretty( pg_total_relation_size(n.nspname||'.'||relname)) as size_new
   FROM pg_class
   JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
   join pg_catalog.pg_user u on relowner=u.usesysid 
   WHERE relpages >= 0
   --and n.nspname in ('truven')
   --and n.nspname = 'data_warehouse'
   --and relname like 'wc_claim%'
   --and u.usename = 'wcough'
   ORDER BY 3, 6 desc;
  
  select * 
  from gp_distribution_policy;

--Greenplum Distribution of a table
SELECT get_ao_distribution('optum_zip.confinement')
order by 1;

select count(*)
from dev.jw_replicated_id_table;

SELECT gp_segment_id, count(*)
FROM dev.jw_replicated_id_table 
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

--Server Settings
SELECT *
FROM   pg_settings
WHERE  name like '%gp_vmem_protect_limit%'; or name like'gp_%';

set gp_workfile_compress_algorithm to 'zlib';

-- Missing Statistics
SELECT * FROM gp_toolkit.gp_stats_missing;

-- Dead Space
SELECT * FROM gp_toolkit.gp_bloat_diag;

--Distribution Keys

create view qa_reporting.distribution_keys as
SELECT
	pgn.nspname as schema_name
	,pgc.relname as table_name
	,COALESCE(pga.attname,'DISTRIBUTED RANDOMLY') as distribution_keys
from pg_catalog.gp_distribution_policy dp
JOIN pg_class AS pgc ON dp.localoid = pgc.oid
JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
LEFT OUTER JOIN pg_attribute pga ON dp.localoid = pga.attrelid and (pga.attnum = dp.distkey[0] or pga.attnum = dp.distkey[1] or pga.attnum = dp.distkey[2])
--where pgn.nspname in ('data_warehouse')-- and pgc.relname = 'temp_script_id'
ORDER BY pgn.nspname, pgc.relname;

alter view qa_reporting.distribution_keys owner to uthealth_dev;

--Compression
drop view qa_reporting.compression_status;
create view qa_reporting.compression_status as
SELECT  b.nspname||'.'||a.relname as TableName
,CASE c.columnstore
   when 'f' THEN 'Row Orientation'        
   when 't' THEN 'Column Orientation'
END as TableStorageType
,pg_size_pretty( pg_total_relation_size(nspname||'.'||relname)) as size_gb
,CASE COALESCE(c.compresstype,'')
  WHEN '' THEN 'No Compression'        
   else c.compresstype
END as CompressionType
FROM pg_class a, pg_namespace b
,(SELECT relid,columnstore,compresstype 
  FROM pg_appendonly) c
WHERE b.oid=a.relnamespace
--and b.nspname not in ('information_schema', 'dbo', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog', 'public', 'qa_reporting', 'dev')  
and b.nspname in ('dw_staging')
AND a.oid=c.relid;

--Roles and Members
create view qa_reporting.user_roles as
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
and t.rarolename like 'uthealth%'
ORDER BY t.rarolename, t.ramembername

SELECT usename AS role_name,
  CASE 
     WHEN usesuper AND usecreatedb THEN 
	   CAST('superuser, create database' AS pg_catalog.text)
     WHEN usesuper THEN 
	    CAST('superuser' AS pg_catalog.text)
     WHEN usecreatedb THEN 
	    CAST('create database' AS pg_catalog.text)
     ELSE 
	    CAST('' AS pg_catalog.text)
  END role_attributes
FROM pg_catalog.pg_user
ORDER BY role_name desc;

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
where t.schemaname in ('data_warhouse')
ORDER BY 1;

select pg_relation_size('dw_qa.claim_detail');

--Last Commit time
show track_commit_timestamp;


--Vacuum Analyze Status
create or replace view qa_reporting.data_warehouse_last_vacuumed
as
SELECT pn.nspname
              ,pc.relname
              ,pslo.staactionname
              ,pslo.stasubtype
              ,pslo.statime as action_date
FROM pg_stat_last_operation pslo
RIGHT OUTER JOIN pg_class pc
ON pc.oid = pslo.objid 
AND pslo.staactionname 
IN ('VACUUM')
INNER JOIN pg_namespace pn
ON pn.oid = pc.relnamespace
WHERE pc.relkind IN ('r','s')
AND pc.relstorage IN ('h', 'a', 'c')
and nspname in ('data_warehouse')
order by 1, 2, 3;

analyze data_warehouse.member_enrollment_yearly;


---see last vacuum and last analyze status of tables
select schemaname, relname, 
       last_vacuum, last_analyze,
       last_autovacuum, last_autoanalyze,
       n_live_tup, n_dead_tup, 
       vacuum_count, autovacuum_count, 
       analyze_count, autoanalyze_count
from pg_stat_user_tables
where schemaname = 'data_warehouse'
order by relname;

--table last operation
-- if table is not in this list, then it means that it has been deleted
--create view qa_reporting.table_last_operation as
select statime, staactionname, stausename, relname table_name, nspname schema_name
from pg_catalog.pg_stat_last_operation a
left join pg_catalog.pg_class b
on a.objid = b.oid
left join pg_catalog.pg_namespace c
on b.relnamespace = c.oid
order by statime desc;

