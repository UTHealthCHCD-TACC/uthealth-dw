/* note that EstimatedRows is based on table statistics - run vacuum and analyze in order to get accurate counts */
SELECT   
	b.nspname||'.'||a.relname as TableName
	,case c.columnstore
		when 'f' then 'Row Orientation'        
		when 't' then 'Column Orientation'
	end as TableStorageType
	,case COALESCE(c.compresstype,'')
		when '' then 'No Compression'        
		else c.compresstype
	end as CompressionType,
	a.reltuples as EstimatedRows
FROM pg_class a
,pg_namespace b
,(select relid,segrelid,columnstore,compresstype from pg_catalog.pg_appendonly) c
WHERE b.oid=a.relnamespace
	and a.oid=c.relid
	and not a.relname like 'z_%'
order by b.nspname
