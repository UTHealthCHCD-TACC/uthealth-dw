/* ******************************************************************************************************
 *  Adds index to tables
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  gmunoz1|| 03/17/2022 || add pharmacy index
 * ****************************************************************************************************** 
 * */



create index claim_diag_diag_cd_idx on data_warehouse.claim_diag using bitmap (diag_cd);

analyze data_warehouse.claim_diag;


create index claim_icd_proc_proc_cd_idx on data_warehouse.claim_icd_proc using bitmap (proc_cd);

analyze data_warehouse.claim_icd_proc;

create index pharmacy_ndc_idx on data_warehouse.pharmacy_claims using bitmap (ndc);

analyze data_warehouse.pharmacy_claims;
