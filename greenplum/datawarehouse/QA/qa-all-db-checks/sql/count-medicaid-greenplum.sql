drop table if exists medicaid.table_counts;

 with all_tables as (
	 select 'greenplum' as db, 'chip_prov' as table_name, year_fy, count(*) as cnt 
	    from medicaid.chip_prov  group by year_fy
	 union all
	 select 'greenplum' as db, 'chip_rx' as table_name, year_fy, count(*) as cnt 
	    from medicaid.chip_rx  group by year_fy
	 union all
	 select 'greenplum' as db, 'chip_uth' as table_name, year_fy, count(*) as cnt 
	    from medicaid.chip_uth  group by year_fy
	 union all
	 select 'greenplum' as db, 'clm_detail' as table_name, year_fy, count(*) as cnt 
	    from medicaid.clm_detail  group by year_fy
	 union all
	 select 'greenplum' as db, 'clm_dx' as table_name, year_fy, count(*) as cnt 
	     from medicaid.clm_dx  group by year_fy
	 union all
	 select 'greenplum' as db, 'clm_header' as table_name, year_fy, count(*) as cnt 
	    from medicaid.clm_header  group by year_fy
	 union all
	 select 'greenplum' as db, 'clm_proc' as table_name, year_fy, count(*) as cnt 
	    from medicaid.clm_proc  group by year_fy
	 union all
	 select 'greenplum' as db, 'enc_det' as table_name, year_fy, count(*) as cnt 
	    from medicaid.enc_det  group by year_fy
	 union all
	 select 'greenplum' as db, 'enc_dx' as table_name, year_fy, count(*) as cnt 
	    from medicaid.enc_dx  group by year_fy
	 union all
	 select 'greenplum' as db, 'enc_header' as table_name, year_fy, count(*) as cnt 
	    from medicaid.enc_header  group by year_fy
	 union all
	 select 'greenplum' as db, 'enc_proc' as table_name, year_fy, count(*) as cnt 
	    from medicaid.enc_proc  group by year_fy
	 union all
	 select 'greenplum' as db, 'enrl' as table_name, year_fy, count(*) as cnt 
	    from medicaid.enrl  group by year_fy
	 union all
	 select 'greenplum' as db, 'ffs_rx' as table_name, year_fy, count(*) as cnt 
	    from medicaid.ffs_rx  group by year_fy
	 union all
	 select 'greenplum' as db, 'mco_rx' as table_name, year_fy, count(*) as cnt 
	    from medicaid.mco_rx  group by year_fy
	 union all
	 select 'greenplum' as db, 'prov' as table_name, year_fy, count(*) as cnt 
	    from medicaid.prov  group by year_fy
	 union all 
	 select 'greenplum' as db, 'htw_clm_detail' as table_name, 1819 as year, count(*) as cnt 
	    from medicaid.htw_clm_detail
	 union all 
	 select 'greenplum' as db, 'htw_clm_dx' as table_name, 1819 as year, count(*) as cnt 
	    from medicaid.htw_clm_dx
	 union all 
	 select 'greenplum' as db, 'htw_clm_header' as table_name, 1819 as year, count(*) as cnt 
	    from medicaid.htw_clm_header
	 union all 
	 select 'greenplum' as db, 'htw_clm_proc' as table_name, 1819 as year, count(*) as cnt 
	    from medicaid.htw_clm_proc
	 union all 
	 select 'greenplum' as db, 'htw_enrl' as table_name, 1819 as year, count(*) as cnt 
	    from medicaid.htw_enrl
	 union all 
	 select 'greenplum' as db, 'htw_ffs_rx' as table_name, 1819 as year, count(*) as cnt 
	    from medicaid.htw_ffs_rx
 )
 select * 
 into medicaid.table_counts
 from all_tables ; 




select * from medicaid.table_counts;

