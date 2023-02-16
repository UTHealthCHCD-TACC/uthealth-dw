select *, relname, relacl from pg_class where relname like 'member_enrollment_monthly%'


SELECT
 pg_size_pretty(pg_relation_size('sql_class.Subscribers')) 
                                                as Subscribers_Heap_Size               
,pg_size_pretty(pg_relation_size('sql_class.wc_Subscribers_Columnar_zlib5')) 
                                               as Subscribers_Columnar_zlib5_Size
,pg_size_pretty(pg_relation_size('sql_class.wc_Subscribers_Columnar_zlib5_rle')) 
                                               as Subscribers_Columnar_zlib5_rle_Size
      
                                               
                                               
               select * from pg_roles;
       
              
              
 SELECT     pgn.nspname as table_owner, pgc.relname as table_name
,COALESCE(pga.attname,'DISTRIBUTED RANDOMLY') as DIST_KEYS
from (
	select a.localoid,
	       case when ( array_upper(a.distkey,1)>0) then unnest(a.distkey) else null end as attnum
	   FROM gp_distribution_policy a 
	   order by a.localoid 
 ) as distrokey  
INNER JOIN pg_class AS pgc
    ON distrokey.localoid = pgc.oid
INNER JOIN pg_namespace pgn
    ON pgc.relnamespace = pgn.oid
LEFT OUTER JOIN pg_attribute pga
    ON distrokey.attnum = pga.attnum
        and distrokey.localoid = pga.attrelid
ORDER BY pgn.nspname, pgc.relname;             
       
select * from pg_namespace


select * from pg


select * from pg_catalog.pg_attribute


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
from tomco to everyone:
SELECT (gdr.segment_file_num/128)+1 as ColumnNumber
,isc.column_name as ColumnName      
,lpad(pg_size_pretty(sum(gdr.mirror_append_only_new_eof)::bigint),15)  as SizeOfColumn
FROM gp_dist_random('gp_persistent_relation_node') gdr
INNER JOIN
  ( SELECT n.nspname, c.relname, relfilenode
    FROM   pg_class c, pg_namespace n
    WHERE  n.oid = c.relnamespace              
    AND    c.relkind = 'r'
    AND    c.relstorage = 'c'
   ) parts
on (gdr.relfilenode_oid = parts.relfilenode)
inner join information_schema.columns isc
on ((gdr.segment_file_num/128)+1) = isc.ordinal_position
and parts.nspname = isc.table_schema
and parts.relname = isc.table_name
group by 1,2
ORDER BY 1,2



SELECT (gdr.segment_file_num/128)+1 as ColumnNumber
,isc.column_name as ColumnName      
,lpad(pg_size_pretty(sum(gdr.mirror_append_only_new_eof)::bigint),15)  as SizeOfColumn
FROM gp_dist_random('gp_persistent_relation_node') gdr
INNER JOIN
  ( SELECT n.nspname, c.relname, relfilenode
    FROM   pg_class c, pg_namespace n
    WHERE  n.oid = c.relnamespace              
    AND    c.relkind = 'r'
    AND    c.relstorage = 'c'
   ) parts
on (gdr.relfilenode_oid = parts.relfilenode)
inner join information_schema.columns isc
on ((gdr.segment_file_num/128)+1) = isc.ordinal_position
and parts.nspname = isc.table_schema
and parts.relname = isc.table_name
group by 1,2
ORDER BY 1,2


select * from gp_catalog



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
AND pc.relstorage IN ('h', 'a', 'c');



vacuum sql_class.employee_table_wc_b


SELECT gwe.datname as DatabaseName, psa.usename as UserName
,gwe.pid as ProcessID, gwe.sess_id as SessionID
,sc.hostname as HostName
,sum(size)/1024::float as SizePerHost
,sum(numfiles) NumOfFilesPerHost
FROM  gp_toolkit.gp_workfile_entries as gwe
inner join pg_stat_activity as psa
on psa.pid = gwe.pid
and psa.sess_id = gwe.sess_id,
gp_segment_configuration as sc
,pg_filespace_entry as fe
,pg_database as d
WHERE fe.fsedbid=sc.dbid AND gwe.segid=sc.content
AND gwe.datname=d.datname AND sc.role='p'
group by gwe.datname, psa.usename, gwe.procpid,
gwe.sess_id, sc.hostname
ORDER BY gwe.datname, psa.usename, gwe.procpid,
gwe.sess_id, sc.hostname




SELECT gp_segment_id, count(*)
FROM dw_qa.claim_detail
GROUP BY gp_segment_id;


SELECT  b.nspname||'.'||a.relname as TableName
,CASE c.columnstore
   when 'f' THEN 'Row Orientation'        
   when 't' THEN 'Column Orientation'
END as TableStorageType
, c.columnstore
,pg_size_pretty( pg_total_relation_size(nspname||'.'||relname)) as size_gb
,CASE COALESCE(c.compresstype,'')
  WHEN '' THEN 'No Compression'        
   else c.compresstype
END as CompressionType
FROM pg_class a, pg_namespace b
,(SELECT relid,columnstore,compresstype 
  FROM pg_appendonly) c
WHERE b.oid=a.relnamespace
and b.nspname in ('data_warehouse')
AND a.oid=c.relid

select *, oid from pg_namespace




select * from pg_catalog.pg_appendonly;


with cte AS ( SELECT  pgn.nspname as SchemaName                 
,pgc.relname as TableName, pga.attname as DistributionType
FROM
(SELECT gdp.localoid,
CASE
 WHEN ( Array_upper(gdp.distkey, 1) > 0 ) THEN Unnest(gdp.distkey)
 ELSE NULL
 END AS distkey
FROM gp_distribution_policy gdp
ORDER BY gdp.localoid
) AS distrokey
INNER JOIN pg_class AS pgc ON distrokey.localoid = pgc.oid
INNER JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
LEFT OUTER JOIN pg_attribute pga ON distrokey.distkey = pga.attnum
AND distrokey.localoid = pga.attrelid      
)select SchemaName, TableName      
,COALESCE(DistributionType,'DISTRIBUTED RANDOMLY') 
AS DistributionType from cte
where COALESCE(DistributionType,'DISTRIBUTED RANDOMLY')=
'DISTRIBUTED RANDOMLY' ORDER BY SchemaName,TableName


select * from pg_catalog.gp_distribution_policy


select distinct dfhostname, dfdevice, pg_size_pretty(dfspace) from gp_toolkit.gp_disk_free


SELECT  spcname as tblspc
               ,fsname as filespc
               ,fsedbid as seg_dbid
               ,fselocation as datadir
FROM pg_tablespace pgts
,pg_filespace pgfs
,pg_filespace_entry pgfse
WHERE pgts.spcfsoid=pgfse.fsefsoid
AND pgfse.fsefsoid=pgfs.oid
ORDER BY tblspc, seg_dbid



SELECT sodddatname, (sodddatsize/1048576.0) AS Size_in_MB
FROM gp_toolkit.gp_size_of_database;
 
SELECT sodddatname, (sodddatsize/1073741824.0) AS Size_in_GB
FROM gp_toolkit.gp_size_of_database;
 
SELECT sodddatname, (sodddatsize/1073741824.0)/1024.0 AS Size_in_TB
FROM gp_toolkit.gp_size_of_database;



SELECT sosdnsp, (sosdschematablesize/1048576) AS Size_in_MB
FROM gp_toolkit.gp_size_of_schema_disk

SELECT sosdnsp, (sosdschematablesize/1073741824) AS Size_in_GB
FROM gp_toolkit.gp_size_of_schema_disk

SELECT sosdnsp, (sosdschematablesize/1073741824)/1024 AS Size_in_TB
FROM gp_toolkit.gp_size_of_schema_disk;

---

Select sh.ctime, query_text, username, db as DatabaseName
   ,rsqname as ResourceQueueName
   ,avg(tfinish-tstart) as AverageRunTime
   ,count(*) as TotalExecution
   ,round(avg(100 - cpu_idle)::numeric,2) as AverageCPUUsed
   ,round(max(100 - cpu_idle)::numeric,2) as MaxCPUUsed
   ,round(avg(mem_actual_used)/power(1024,3)::numeric,2) AverageMemoryUsed
   ,round(max(mem_actual_used)/power(1024,3)::numeric,2) as MaxMemoryUsed
from system_history sh, queries_history qh
where sh.ctime between date_trunc('day',localtimestamp - interval '10 days')
and date_trunc('day',localtimestamp)
and sh.ctime=qh.ctime
and db not in ('gpperfmon')
and date_part('hour',tfinish - tstart)*60 + date_part('minute',tfinish - tstart) > 20
group by sh.ctime,query_text,username, db,rsqname;

SELECT
a.sotuschemaname as Schema,
a.sotutablename as Table,
pg_size_pretty(a.sotusize::BIGINT) as Size,
b.tableowner
FROM gp_toolkit.gp_size_of_table_uncompressed a
JOIN pg_tables b ON (a.sotutablename = b.tablename)
WHERE a.sotuschemaname = 'sql_class'
ORDER BY sotusize DESC
LIMIT 50;


select version();




select *
from gp_toolkit.gp_skew_coefficients
where skcnamespace = 'dw_qa'
