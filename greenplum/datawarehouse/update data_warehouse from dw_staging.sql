
/* ******************************************************************************************************
 *  This script will move updated tables from dw_staging to data_warehouse. This is the final step in the update process
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wc001  || 9/29/2021 || script creation
 * ****************************************************************************************************** 
 *  wc002  || 10/05/2021 || add mbsf_abcd table
 * ****************************************************************************************************** 
 * */

----rename existing production tables to _old
alter table data_warehouse.claim_header rename to claim_header_old; 

alter table data_warehouse.claim_detail rename to claim_detail_old; 

alter table data_warehouse.claim_diag rename to claim_diag_old; 

alter table data_warehouse.claim_icd_proc rename to claim_icd_proc_old;

alter table data_warehouse.pharmacy_claims rename to pharmacy_claims_old; 

alter table data_warehouse.member_enrollment_monthly rename to member_enrollment_monthly_old;

alter table data_warehouse.member_enrollment_yearly rename to member_enrollment_yearly_old;

alter table data_warehouse.medicare_mbsf_abcd_enrollment rename to medicare_mbsf_abcd_enrollment_old;

alter table data_warehouse.medicaid_program_enrollment rename to medicaid_program_enrollment_old;
----


-----//   move staging tables to production
alter table dw_staging.claim_header set schema data_warehouse;

alter table dw_staging.claim_detail set schema data_warehouse;

alter table dw_staging.claim_diag set schema data_warehouse;

alter table dw_staging.claim_icd_proc  set schema data_warehouse;

alter table dw_staging.pharmacy_claims set schema data_warehouse;

alter table dw_staging.member_enrollment_monthly set schema data_warehouse;

alter table dw_staging.member_enrollment_yearly set schema data_warehouse;

alter table dw_staging.medicare_mbsf_abcd_enrollment set schema data_warehouse;

alter table dw_staging.medicaid_program_enrollment set schema data_warehouse;
-----//




-----||  move old tables  
alter table  data_warehouse.claim_header_old set schema dw_staging;

alter table  data_warehouse.claim_detail_old set schema dw_staging;

alter table  data_warehouse.claim_diag_old set schema dw_staging;

alter table data_warehouse.claim_icd_proc_old set schema dw_staging;

alter table  data_warehouse.pharmacy_claims_old set schema dw_staging;

alter table  data_warehouse.member_enrollment_monthly_old set schema dw_staging;

alter table  data_warehouse.member_enrollment_yearly_old set schema dw_staging;

alter table  data_warehouse.medicare_mbsf_abcd_enrollment_old set schema dw_staging;

alter table data_warehouse.medicaid_program_enrollment_old set schema dw_staging;
-----||




