-- Generate Query
select 'select '''||tablename||''' as table_name, year, count(*) as cnt from '||schemaname||'.'||tablename||' group by 1, 2 union'
from pg_tables where schemaname in ('medicare_texas')
order by schemaname, tablename;

--Get Stats
select table_name, year, cnt
from medicare_texas.table_counts
order by 1, 2;
-- Save Results Query
drop table medicare_texas.table_counts;
create table medicare_texas.table_counts
as
select 'bcarrier_claims_k' as table_name, year, count(*) as cnt from medicare_texas.bcarrier_claims_k group by 1, 2 union
select 'bcarrier_demo_codes' as table_name, year, count(*) as cnt from medicare_texas.bcarrier_demo_codes group by 1, 2 union
select 'bcarrier_line_k' as table_name, year, count(*) as cnt from medicare_texas.bcarrier_line_k group by 1, 2 union
select 'dme_claims_k' as table_name, year, count(*) as cnt from medicare_texas.dme_claims_k group by 1, 2 union
select 'dme_demo_codes' as table_name, year, count(*) as cnt from medicare_texas.dme_demo_codes group by 1, 2 union
select 'dme_line_k' as table_name, year, count(*) as cnt from medicare_texas.dme_line_k group by 1, 2 union
select 'hha_base_claims_k' as table_name, year, count(*) as cnt from medicare_texas.hha_base_claims_k group by 1, 2 union
select 'hha_condition_codes' as table_name, year, count(*) as cnt from medicare_texas.hha_condition_codes group by 1, 2 union
select 'hha_demo_codes' as table_name, year, count(*) as cnt from medicare_texas.hha_demo_codes group by 1, 2 union
select 'hha_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_texas.hha_occurrnce_codes group by 1, 2 union
select 'hha_revenue_center_k' as table_name, year, count(*) as cnt from medicare_texas.hha_revenue_center_k group by 1, 2 union
select 'hha_span_codes' as table_name, year, count(*) as cnt from medicare_texas.hha_span_codes group by 1, 2 union
select 'hha_value_codes' as table_name, year, count(*) as cnt from medicare_texas.hha_value_codes group by 1, 2 union
select 'hospice_base_claims_k' as table_name, year, count(*) as cnt from medicare_texas.hospice_base_claims_k group by 1, 2 union
select 'hospice_condition_codes' as table_name, year, count(*) as cnt from medicare_texas.hospice_condition_codes group by 1, 2 union
select 'hospice_demo_codes' as table_name, year, count(*) as cnt from medicare_texas.hospice_demo_codes group by 1, 2 union
select 'hospice_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_texas.hospice_occurrnce_codes group by 1, 2 union
select 'hospice_revenue_center_k' as table_name, year, count(*) as cnt from medicare_texas.hospice_revenue_center_k group by 1, 2 union
select 'hospice_span_codes' as table_name, year, count(*) as cnt from medicare_texas.hospice_span_codes group by 1, 2 union
select 'hospice_value_codes' as table_name, year, count(*) as cnt from medicare_texas.hospice_value_codes group by 1, 2 union
select 'inpatient_base_claims_k' as table_name, year, count(*) as cnt from medicare_texas.inpatient_base_claims_k group by 1, 2 union
select 'inpatient_condition_codes' as table_name, year, count(*) as cnt from medicare_texas.inpatient_condition_codes group by 1, 2 union
select 'inpatient_demo_codes' as table_name, year, count(*) as cnt from medicare_texas.inpatient_demo_codes group by 1, 2 union
select 'inpatient_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_texas.inpatient_occurrnce_codes group by 1, 2 union
select 'inpatient_revenue_center_k' as table_name, year, count(*) as cnt from medicare_texas.inpatient_revenue_center_k group by 1, 2 union
select 'inpatient_span_codes' as table_name, year, count(*) as cnt from medicare_texas.inpatient_span_codes group by 1, 2 union
select 'inpatient_value_codes' as table_name, year, count(*) as cnt from medicare_texas.inpatient_value_codes group by 1, 2 union
select 'mbsf_abcd_summary' as table_name, year, count(*) as cnt from medicare_texas.mbsf_abcd_summary group by 1, 2 union
select 'outpatient_base_claims_k' as table_name, year, count(*) as cnt from medicare_texas.outpatient_base_claims_k group by 1, 2 union
select 'outpatient_condition_codes' as table_name, year, count(*) as cnt from medicare_texas.outpatient_condition_codes group by 1, 2 union
select 'outpatient_demo_codes' as table_name, year, count(*) as cnt from medicare_texas.outpatient_demo_codes group by 1, 2 union
select 'outpatient_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_texas.outpatient_occurrnce_codes group by 1, 2 union
select 'outpatient_revenue_center_k' as table_name, year, count(*) as cnt from medicare_texas.outpatient_revenue_center_k group by 1, 2 union
select 'outpatient_span_codes' as table_name, year, count(*) as cnt from medicare_texas.outpatient_span_codes group by 1, 2 union
select 'outpatient_value_codes' as table_name, year, count(*) as cnt from medicare_texas.outpatient_value_codes group by 1, 2 union
select 'pde_file' as table_name, year, count(*) as cnt from medicare_texas.pde_file group by 1, 2 union
select 'ptab_samhsa_xwalk' as table_name, year, count(*) as cnt from medicare_texas.ptab_samhsa_xwalk group by 1, 2 union
select 'snf_base_claims_k' as table_name, year, count(*) as cnt from medicare_texas.snf_base_claims_k group by 1, 2 union
select 'snf_condition_codes' as table_name, year, count(*) as cnt from medicare_texas.snf_condition_codes group by 1, 2 union
select 'snf_demo_codes' as table_name, year, count(*) as cnt from medicare_texas.snf_demo_codes group by 1, 2 union
select 'snf_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_texas.snf_occurrnce_codes group by 1, 2 union
select 'snf_revenue_center_k' as table_name, year, count(*) as cnt from medicare_texas.snf_revenue_center_k group by 1, 2 union
select 'snf_span_codes' as table_name, year, count(*) as cnt from medicare_texas.snf_span_codes group by 1, 2 union
select 'snf_value_codes' as table_name, year, count(*) as cnt from medicare_texas.snf_value_codes group by 1, 2