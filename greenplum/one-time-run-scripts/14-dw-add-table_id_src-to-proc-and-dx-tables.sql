/***********************************
 * This script adds a table_id_src column to the claim_diag and claim_icd_proc tables in data_warehouse
 * 
 * Xiaorui 7/17/2023
 */

--claim_diag
alter table data_warehouse.claim_diag
add column table_id_src varchar(100);

vacuum analyze data_warehouse.claim_diag;

--claim_icd_proc
alter table data_warehouse.claim_icd_proc
add column table_id_src varchar(100);

vacuum analyze data_warehouse.claim_icd_proc;






