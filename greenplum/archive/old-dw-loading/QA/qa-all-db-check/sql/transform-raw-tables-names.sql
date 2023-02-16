
 
drop table if exists dev.medicaid_counts_sqlserver; 
 
with lowers as (
	select counts, lower(tabname) as tabname 
  from dev.mcd_counts_sql_server
 )
 select 'sqlserver' as db, counts, tabname, 
   case 
   	  when tabname like '%clm_dx%' and tabname like '%htw%' then 'htw_clm_dx'
      when tabname like '%clm_dx%' then 'clm_dx'
      when tabname like '%chip_prov%' then 'chip_prov'
      when tabname like '%chip%' and tabname like '%rx%' then 'chip_rx'
      when tabname like '%chip_uth%' then 'chip_uth'
      when tabname like '%clm_detail%' and tabname like '%htw%' then 'htw_clm_detail'
      when tabname like '%clm_detail%' then 'clm_detail'
      when tabname like '%clm_header%' and tabname like '%htw%' then 'htw_clm_header'
      when tabname like '%clm_header%' then 'clm_header'
      when tabname like '%clm_proc%' and tabname like '%htw%' then 'htw_clm_proc'
      when tabname like '%clm_proc%' then 'clm_proc'
      when tabname like '%enc_det%' then 'enc_det'
      when tabname like '%enc_dx%' then 'enc_dx'
      when tabname like '%enc_header%' then 'enc_header'
      when tabname like '%enc_proc%' then 'enc_proc'
      when tabname like '%enrl%' and tabname like '%htw%' then 'htw_enrl'
      when tabname like '%enrl%' then 'enrl'
      when tabname like '%ffs_rx%' then 'ffs_rx'
      when tabname like '%mco_rx%' then 'mco_rx'
      when tabname like '%prov%' then 'prov'
      when tabname like '%ffs_rx%' and tabname like '%htw%' then 'htw_ffs_rx'
      when tabname like '%ffs_rx%' then 'ffs_rx'  
      when tabname like '%ffs_rx%' then 'ffs_rx'  
   end as table_name,
 	case 
 	  when tabname like '%1819%' or tabname like '%18_19%' then '1819'
      when tabname like '%12%' then '2012'
      when tabname like '%13%' then '2013'
      when tabname like '%14%' then '2014'
      when tabname like '%15%' then '2015'
      when tabname like '%16%' then '2016'
      when tabname like '%17%' then '2017'
      when tabname like '%18%' then '2018'
      when tabname like '%19%' then '2019'
      when tabname like '%21%' then '2021'
      when tabname like '%20%' then '2020' 
   end as year
   into dev.medicaid_counts_sqlserver
   from lowers
  ;

 
select * from  dev.medicaid_counts_sqlserver order by sql_server_table_name ; 
 
  select  a.table_name, year_fy, b.year, cnt as greenplum_count, 
         b.counts as sql_server_count, cnt - counts as difference,  b.tabname as sql_server_table_name
    from dev.medicaid_counts_sqlserver b 
    left outer join  medicaid.table_counts a 
      on a.year_fy::text = b.year::text
     and a.table_name = b.table_name ;
 
 