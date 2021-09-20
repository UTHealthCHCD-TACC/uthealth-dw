-- Generate Query
select 'select '''||tablename||''' as table_name, year, count(*) as cnt from '||schemaname||'.'||tablename||' group by 1, 2 union'
from pg_tables where schemaname in ('medicare_national')
order by schemaname, tablename;

--Get Stats
select table_name, year, cnt
from uthealth/medicare_national.table_counts
order by 1, 2;
-- Save Results Query
drop table medicare_national.table_counts;
create table medicare_national.table_counts
as
--select 'admit' as table_name, year, count(*) as cnt from medicare_national.admit group by 1, 2 union
--select 'admit_clm' as table_name, year, count(*) as cnt from medicare_national.admit_clm group by 1, 2 union
select 'bcarrier_claims_k' as table_name, year, count(*) as cnt from medicare_national.bcarrier_claims_k group by 1, 2 union
select 'bcarrier_line_k' as table_name, year, count(*) as cnt from medicare_national.bcarrier_line_k group by 1, 2 union
select 'dme_claims_k' as table_name, year, count(*) as cnt from medicare_national.dme_claims_k group by 1, 2 union
select 'dme_line_k' as table_name, year, count(*) as cnt from medicare_national.dme_line_k group by 1, 2 union
select 'hha_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.hha_base_claims_k group by 1, 2 union
select 'hha_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.hha_revenue_center_k group by 1, 2 union
select 'hospice_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.hospice_base_claims_k group by 1, 2 union
select 'hospice_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.hospice_revenue_center_k group by 1, 2 union
select 'inpatient_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.inpatient_base_claims_k group by 1, 2 union
select 'inpatient_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.inpatient_revenue_center_k group by 1, 2 union
select 'mbsf_abcd_summary' as table_name, year, count(*) as cnt from medicare_national.mbsf_abcd_summary group by 1, 2 union
select 'outpatient_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.outpatient_base_claims_k group by 1, 2 union
select 'outpatient_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.outpatient_revenue_center_k group by 1, 2 union
select 'pde_file' as table_name, year, count(*) as cnt from medicare_national.pde_file group by 1, 2 union
select 'snf_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.snf_base_claims_k group by 1, 2 union
select 'snf_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.snf_revenue_center_k group by 1, 2
;