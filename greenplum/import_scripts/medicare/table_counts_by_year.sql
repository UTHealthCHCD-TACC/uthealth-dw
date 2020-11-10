-- Generate Query
select 'select '''||tablename||''' as table_name, year, count(*) as cnt from '||schemaname||'.'||tablename||' group by 1, 2 union'
from pg_tables where schemaname in ('medicare_national')
order by schemaname, tablename;

--Get Stats
select table_name, year, cnt
from medicare_national.table_counts
order by 1, 2;
-- Save Results Query
drop table medicare_national.table_counts;
create table medicare_national.table_counts
as
select 'bcarrier_claims_k' as table_name, year, count(*) as cnt from medicare_national.bcarrier_claims_k group by 1, 2 union
select 'bcarrier_demo_codes' as table_name, year, count(*) as cnt from medicare_national.bcarrier_demo_codes group by 1, 2 union
select 'bcarrier_line_k' as table_name, year, count(*) as cnt from medicare_national.bcarrier_line_k group by 1, 2 union
select 'dme_claims_k' as table_name, year, count(*) as cnt from medicare_national.dme_claims_k group by 1, 2 union
select 'dme_demo_codes' as table_name, year, count(*) as cnt from medicare_national.dme_demo_codes group by 1, 2 union
select 'dme_line_k' as table_name, year, count(*) as cnt from medicare_national.dme_line_k group by 1, 2 union
select 'hha_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.hha_base_claims_k group by 1, 2 union
select 'hha_condition_codes' as table_name, year, count(*) as cnt from medicare_national.hha_condition_codes group by 1, 2 union
select 'hha_demo_codes' as table_name, year, count(*) as cnt from medicare_national.hha_demo_codes group by 1, 2 union
select 'hha_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_national.hha_occurrnce_codes group by 1, 2 union
select 'hha_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.hha_revenue_center_k group by 1, 2 union
select 'hha_span_codes' as table_name, year, count(*) as cnt from medicare_national.hha_span_codes group by 1, 2 union
select 'hha_value_codes' as table_name, year, count(*) as cnt from medicare_national.hha_value_codes group by 1, 2 union
select 'hospice_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.hospice_base_claims_k group by 1, 2 union
select 'hospice_condition_codes' as table_name, year, count(*) as cnt from medicare_national.hospice_condition_codes group by 1, 2 union
select 'hospice_demo_codes' as table_name, year, count(*) as cnt from medicare_national.hospice_demo_codes group by 1, 2 union
select 'hospice_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_national.hospice_occurrnce_codes group by 1, 2 union
select 'hospice_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.hospice_revenue_center_k group by 1, 2 union
select 'hospice_span_codes' as table_name, year, count(*) as cnt from medicare_national.hospice_span_codes group by 1, 2 union
select 'hospice_value_codes' as table_name, year, count(*) as cnt from medicare_national.hospice_value_codes group by 1, 2 union
select 'inpatient_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.inpatient_base_claims_k group by 1, 2 union
select 'inpatient_condition_codes' as table_name, year, count(*) as cnt from medicare_national.inpatient_condition_codes group by 1, 2 union
select 'inpatient_demo_codes' as table_name, year, count(*) as cnt from medicare_national.inpatient_demo_codes group by 1, 2 union
select 'inpatient_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_national.inpatient_occurrnce_codes group by 1, 2 union
select 'inpatient_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.inpatient_revenue_center_k group by 1, 2 union
select 'inpatient_span_codes' as table_name, year, count(*) as cnt from medicare_national.inpatient_span_codes group by 1, 2 union
select 'inpatient_value_codes' as table_name, year, count(*) as cnt from medicare_national.inpatient_value_codes group by 1, 2 union
select 'mbsf_abcd_summary' as table_name, year, count(*) as cnt from medicare_national.mbsf_abcd_summary group by 1, 2 union
select 'outpatient_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.outpatient_base_claims_k group by 1, 2 union
select 'outpatient_condition_codes' as table_name, year, count(*) as cnt from medicare_national.outpatient_condition_codes group by 1, 2 union
select 'outpatient_demo_codes' as table_name, year, count(*) as cnt from medicare_national.outpatient_demo_codes group by 1, 2 union
select 'outpatient_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_national.outpatient_occurrnce_codes group by 1, 2 union
select 'outpatient_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.outpatient_revenue_center_k group by 1, 2 union
select 'outpatient_span_codes' as table_name, year, count(*) as cnt from medicare_national.outpatient_span_codes group by 1, 2 union
select 'outpatient_value_codes' as table_name, year, count(*) as cnt from medicare_national.outpatient_value_codes group by 1, 2 union
select 'pde_file' as table_name, year, count(*) as cnt from medicare_national.pde_file group by 1, 2 union
select 'ptab_samhsa_xwalk' as table_name, year, count(*) as cnt from medicare_national.ptab_samhsa_xwalk group by 1, 2 union
select 'snf_base_claims_k' as table_name, year, count(*) as cnt from medicare_national.snf_base_claims_k group by 1, 2 union
select 'snf_condition_codes' as table_name, year, count(*) as cnt from medicare_national.snf_condition_codes group by 1, 2 union
select 'snf_demo_codes' as table_name, year, count(*) as cnt from medicare_national.snf_demo_codes group by 1, 2 union
select 'snf_occurrnce_codes' as table_name, year, count(*) as cnt from medicare_national.snf_occurrnce_codes group by 1, 2 union
select 'snf_revenue_center_k' as table_name, year, count(*) as cnt from medicare_national.snf_revenue_center_k group by 1, 2 union
select 'snf_span_codes' as table_name, year, count(*) as cnt from medicare_national.snf_span_codes group by 1, 2 union
select 'snf_value_codes' as table_name, year, count(*) as cnt from medicare_national.snf_value_codes group by 1, 2