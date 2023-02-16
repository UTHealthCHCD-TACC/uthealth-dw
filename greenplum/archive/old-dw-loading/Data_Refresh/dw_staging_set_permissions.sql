/*
 * Set permissions for staging tables 
 * 
 */



alter table dw_staging.member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.claim_header owner to uthealth_dev;
alter table dw_staging.claim_detail owner to uthealth_dev;
alter table dw_staging.claim_diag owner to uthealth_dev;
alter table dw_staging.claim_icd_proc owner to uthealth_dev;
alter table dw_staging.pharmacy_claims owner to uthealth_dev;