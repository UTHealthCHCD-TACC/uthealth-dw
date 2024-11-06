-- Optum Zip Schema
optum_zip.confinement
optum_zip.diagnostic
optum_zip.lab_result
optum_zip.lu_diagnosis
optum_zip.lu_ndc
optum_zip.lu_procedure
optum_zip.mbr_co_enroll
optum_zip.mbr_enroll
optum_zip.medical
optum_zip."procedure"
optum_zip.provider
optum_zip.provider_bridge
optum_zip.rx
optum_zip.table_counts

-- Optum DOD Schema
optum_dod.confinement
optum_dod.diagnostic
optum_dod.lab_result
optum_dod.lu_diagnosis
optum_dod.lu_ndc
optum_dod.lu_procedure
optum_dod.mbr_co_enroll_r
optum_dod.mbr_enroll_r
optum_dod.mbrwdeath
optum_dod.medical
optum_dod."procedure"
optum_dod.provider
optum_dod.provider_bridge
optum_dod.rx
optum_dod.table_counts

-- Data Warehouse

create table dw_staging.optz_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optz_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optz_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optz_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optz_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optz_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optz_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

alter table dw_staging.optz_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.optz_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.optz_claim_detail owner to uthealth_dev;
alter table dw_staging.optz_claim_diag owner to uthealth_dev;
alter table dw_staging.optz_claim_header owner to uthealth_dev;
alter table dw_staging.optz_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.optz_pharmacy_claims owner to uthealth_dev;


alter table data_warehouse.member_enrollment_monthly
exchange partition optz
with table dw_staging.optz_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition optz
with table dw_staging.optz_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition optz
with table dw_staging.optz_claim_detail;

alter table data_warehouse.claim_diag
exchange partition optz
with table dw_staging.optz_claim_diag;

alter table data_warehouse.claim_header
exchange partition optz
with table dw_staging.optz_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition optz
with table dw_staging.optz_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition optz
with table dw_staging.optz_pharmacy_claims;

drop table if exists dw_staging.optz_member_enrollment_monthly;
drop table if exists dw_staging.optz_member_enrollment_yearly;
drop table if exists dw_staging.optz_claim_detail;
drop table if exists dw_staging.optz_claim_diag;
drop table if exists dw_staging.optz_claim_header;
drop table if exists dw_staging.optz_claim_icd_proc;
drop table if exists dw_staging.optz_pharmacy_claims;

create table dw_staging.optd_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optd_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optd_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optd_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optd_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optd_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.optd_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

alter table dw_staging.optd_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.optd_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.optd_claim_detail owner to uthealth_dev;
alter table dw_staging.optd_claim_diag owner to uthealth_dev;
alter table dw_staging.optd_claim_header owner to uthealth_dev;
alter table dw_staging.optd_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.optd_pharmacy_claims owner to uthealth_dev;

alter table data_warehouse.member_enrollment_monthly
exchange partition optd
with table dw_staging.optd_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition optd
with table dw_staging.optd_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition optd
with table dw_staging.optd_claim_detail;

alter table data_warehouse.claim_diag
exchange partition optd
with table dw_staging.optd_claim_diag;

alter table data_warehouse.claim_header
exchange partition optd
with table dw_staging.optd_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition optd
with table dw_staging.optd_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition optd
with table dw_staging.optd_pharmacy_claims;

drop table if exists dw_staging.optd_member_enrollment_monthly;
drop table if exists dw_staging.optd_member_enrollment_yearly;
drop table if exists dw_staging.optd_claim_detail;
drop table if exists dw_staging.optd_claim_diag;
drop table if exists dw_staging.optd_claim_header;
drop table if exists dw_staging.optd_claim_icd_proc;
drop table if exists dw_staging.optd_pharmacy_claims;

delete from data_warehouse.admission_acute_ip where data_source like 'opt%';
delete from data_warehouse.admission_acute_ip_claims where data_source like 'opt%';
delete from data_warehouse.conditions_member_enrollment_yearly where data_source like 'opt%';
delete from data_warehouse.covid_severity where data_source like 'opt%';
delete from data_warehouse.crg_risk where data_source like 'opt%';
delete from data_warehouse.dim_uth_claim_id where data_source like 'opt%';
delete from data_warehouse.dim_uth_member_id where data_source like 'opt%';
delete from data_warehouse.dim_uth_rx_claim_id where data_source like 'opt%';

update data_warehouse.update_log
set data_last_updated = current_date,
last_vacuum_analyze = current_date,
details = 'Deleted Optum data'
where schema_name = 'data_warehouse'
and (
table_name not like '%_prt'
or table_name like '%opt%'
);

-- Conditions
conditions.conditions_member_enrollment_yearly
conditions.person_profile_stage
conditions.person_profile_stage_trv
conditions.person_profile_work_table
conditions.person_profile_work_table_trv
conditions.xl_condition_asthma_dx_1
conditions.xl_condition_asthma_dx_3
conditions.xl_condition_asthma_dx_4
conditions.xl_condition_asthma_dx_output
conditions.xl_condition_diabetes_1
conditions.xl_condition_diabetes_3
conditions.xl_condition_diabetes_output
conditions.xl_condition_htn_dx_1

-- qa_reporting

/*
 * Contains tables with counts of tables
 */

-- tableau

tableau.claim_header_optz_truv
tableau.covid_severity_raw_optd
tableau.crg_risk
tableau.dashboard_1720
tableau.diag_dashboard
tableau.dw_severity_2020
tableau.dw_severity_2021
tableau.enrollment_only
tableau.enrollment_yearly_optz_truv
tableau.master_claims
tableau.master_enrollment
tableau.member_conditions
tableau.member_conditions_old
tableau.tx_claim_header
tableau.tx_covid
tableau.tx_crg_risk
tableau.tx_diag_dashboard
tableau.tx_enrollment
tableau.tx_member_conditions
