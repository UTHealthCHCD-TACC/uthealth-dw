-- Generate Query
select 'select '''||tablename||''' as table_name, year_fy, count(*) as cnt from '||schemaname||'.'||tablename||' group by 1, 2 union'
from pg_tables where schemaname in ('medicaid')
order by schemaname, tablename;

-- Save Results Query
drop table medicaid.table_counts;
create table medicaid.table_counts
as
select 'chip_prov' as table_name, year_fy, count(*) as cnt from medicaid.chip_prov group by 1, 2 union
select 'chip_rx' as table_name, year_fy, count(*) as cnt from medicaid.chip_rx group by 1, 2 union
select 'chip_uth' as table_name, year_fy, count(*) as cnt from medicaid.chip_uth group by 1, 2 union
select 'clm_detail' as table_name, year_fy, count(*) as cnt from medicaid.clm_detail group by 1, 2 union
select 'clm_dx' as table_name, year_fy, count(*) as cnt from medicaid.clm_dx group by 1, 2 union
select 'clm_header' as table_name, year_fy, count(*) as cnt from medicaid.clm_header group by 1, 2 union
select 'clm_proc' as table_name, year_fy, count(*) as cnt from medicaid.clm_proc group by 1, 2 union
select 'enc_det' as table_name, year_fy, count(*) as cnt from medicaid.enc_det group by 1, 2 union
select 'enc_dx' as table_name, year_fy, count(*) as cnt from medicaid.enc_dx group by 1, 2 union
select 'enc_header' as table_name, year_fy, count(*) as cnt from medicaid.enc_header group by 1, 2 union
select 'enc_proc' as table_name, year_fy, count(*) as cnt from medicaid.enc_proc group by 1, 2 union
select 'enrl' as table_name, year_fy, count(*) as cnt from medicaid.enrl group by 1, 2 union
select 'ffs_rx' as table_name, year_fy, count(*) as cnt from medicaid.ffs_rx group by 1, 2 union
select 'mco_rx' as table_name, year_fy, count(*) as cnt from medicaid.mco_rx group by 1, 2 union
select 'prov' as table_name, year_fy, count(*) as cnt from medicaid.prov group by 1, 2;

select *
from medicaid.table_counts
order by 1, 2;