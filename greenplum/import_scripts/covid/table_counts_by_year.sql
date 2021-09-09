-- Generate Query
select 'select '''||tablename||''' as table_name, date_part("year", field) as year, count(*) as cnt from '||schemaname||'.'||tablename||' group by 1, 2 union'
from pg_tables where schemaname in ('opt_20210624')
order by schemaname, tablename;

-- Save Results Query
drop table opt_20210401.table_counts;
create table opt_20210401.table_counts
as
select 'carearea' as table_name, date_part('year', carearea_date) as year, count(*) as cnt from opt_20210401.carearea group by 1, 2 union
select 'diag' as table_name, date_part('year', diag_date) as year, count(*) as cnt from opt_20210401.diag group by 1, 2 union
select 'enc' as table_name, date_part('year', interaction_date) as year, count(*) as cnt from opt_20210401.enc group by 1, 2 union
select 'enc_prov' as table_name, 0 as year, count(*) as cnt from opt_20210401.enc_prov group by 1, 2 union
select 'ins' as table_name, date_part('year', insurance_date) as year, count(*) as cnt from opt_20210401.ins group by 1, 2 union
select 'lab' as table_name, date_part('year', collected_date) as year, count(*) as cnt from opt_20210401.lab group by 1, 2 union
select 'micro' as table_name, date_part('year', collect_date) as year, count(*) as cnt from opt_20210401.micro group by 1, 2 union
select 'obs' as table_name, date_part('year', obs_date) as year, count(*) as cnt from opt_20210401.obs group by 1, 2 union
select 'proc' as table_name, date_part('year', proc_date) as year, count(*) as cnt from opt_20210401.proc group by 1, 2 union
select 'prov' as table_name, 0 as year, count(*) as cnt from opt_20210401.prov group by 1, 2 union
select 'pt' as table_name, 0 as year, count(*) as cnt from opt_20210401.pt group by 1, 2 union
select 'rx_adm' as table_name, date_part('year', admin_date) as year, count(*) as cnt from opt_20210401.rx_adm group by 1, 2 union
select 'rx_immun' as table_name, date_part('year', immunization_date) as year, count(*) as cnt from opt_20210401.rx_immun group by 1, 2 union
select 'rx_patrep' as table_name, date_part('year', reported_date) as year, count(*) as cnt from opt_20210401.rx_patrep group by 1, 2 union
select 'rx_presc' as table_name, date_part('year', rxdate) as year, count(*) as cnt from opt_20210401.rx_presc group by 1, 2 union
select 'vis' as table_name, date_part('year', visit_start_date) as year, count(*) as cnt from opt_20210401.vis group by 1, 2;

select *
from medicaid.table_counts
order by 1, 2;