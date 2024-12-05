/***********************************
* Script purpose: vacuum analyze all Medicare tables
* except for reference tables and table_counts
***********************************/

--when were tables last vacuum analyzed?
select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'medicare_national';

select schemaname, relname, last_vacuum, last_analyze
from pg_stat_all_tables
where schemaname = 'medicare_texas';

--code to generate code to vacuum analyze all tables
select 'vacuum analyze ' || schemaname || '.' || relname || ';'
from pg_stat_all_tables
  where schemaname = 'medicare_national'
  order by n_live_tup;
 
 select 'vacuum analyze ' || schemaname || '.' || relname || ';'
from pg_stat_all_tables
  where schemaname = 'medicare_texas'
  order by n_live_tup;
 
vacuum analyze medicare_national.inpatient_revenue_center_k;
vacuum analyze medicare_national.mbsf_cc_summary;
vacuum analyze medicare_national.hospice_base_claims_k;
-- vacuum analyze medicare_national.pde_file; --not loaded yet
vacuum analyze medicare_national.hospice_revenue_center_k;
vacuum analyze medicare_national.hha_revenue_center_k;
vacuum analyze medicare_national.snf_base_claims_k;
vacuum analyze medicare_national.outpatient_revenue_center_k;
vacuum analyze medicare_national.snf_revenue_center_k;
vacuum analyze medicare_national.dme_claims_k;
vacuum analyze medicare_national.bcarrier_line_k;
vacuum analyze medicare_national.hha_base_claims_k;
vacuum analyze medicare_national.outpatient_base_claims_k;
vacuum analyze medicare_national.dme_line_k;
vacuum analyze medicare_national.bcarrier_claims_k;
vacuum analyze medicare_national.inpatient_base_claims_k;
vacuum analyze medicare_national.mbsf_chronic_summary;
vacuum analyze medicare_national.table_counts;
vacuum analyze medicare_national.mbsf_oth_cc_summary;
vacuum analyze medicare_national.mbsf_costuse;
vacuum analyze medicare_national.mbsf_abcd_summary;

vacuum analyze medicare_texas.inpatient_base_claims_k;
vacuum analyze medicare_texas.snf_revenue_center_k;
vacuum analyze medicare_texas.snf_base_claims_k;
vacuum analyze medicare_texas.outpatient_revenue_center_k;
vacuum analyze medicare_texas.hospice_revenue_center_k;
vacuum analyze medicare_texas.hospice_base_claims_k;
vacuum analyze medicare_texas.mbsf_chronic_summary;
vacuum analyze medicare_texas.dme_line_k;
vacuum analyze medicare_texas.bcarrier_line_k;
vacuum analyze medicare_texas.bcarrier_claims_k;
vacuum analyze medicare_texas.hha_base_claims_k;
vacuum analyze medicare_texas.mbsf_oth_cc_summary;
vacuum analyze medicare_texas.table_counts;
vacuum analyze medicare_texas.pde_file;
vacuum analyze medicare_texas.admit_clm;
vacuum analyze medicare_texas.admit;
vacuum analyze medicare_texas.mbsf_cc_summary;
vacuum analyze medicare_texas.inpatient_revenue_center_k;
vacuum analyze medicare_texas.dme_claims_k;
vacuum analyze medicare_texas.mbsf_costuse;
vacuum analyze medicare_texas.mbsf_abcd_summary;
vacuum analyze medicare_texas.hha_revenue_center_k;
vacuum analyze medicare_texas.outpatient_base_claims_k;
 
 