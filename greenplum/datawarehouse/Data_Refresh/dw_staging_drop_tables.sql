/*
 * Drop staging tables 
 * 
 */

drop table if exists dw_staging.member_enrollment_monthly;
drop table if exists dw_staging.member_enrollment_monthly_old;
drop table if exists dw_staging.member_enrollment_yearly;
drop table if exists dw_staging.member_enrollment_yearly_old;

drop table if exists dw_staging.medicaid_program_enrollment;
drop table if exists dw_staging.medicaid_program_enrollment_old;
drop table if exists dw_staging.medicare_mbsf_abcd_enrollment;
drop table if exists dw_staging.medicare_mbsf_abcd_enrollment_old;

drop table if exists dw_staging.claim_header;
drop table if exists dw_staging.claim_header_old;
drop table if exists dw_staging.claim_detail;
drop table if exists dw_staging.claim_detail_old;
drop table if exists dw_staging.claim_diag;
drop table if exists dw_staging.claim_diag_old;
drop table if exists dw_staging.claim_icd_proc;
drop table if exists dw_staging.claim_icd_proc_old;

drop table if exists dw_staging.pharmacy_claims;
drop table if exists dw_staging.pharmacy_claims_old;