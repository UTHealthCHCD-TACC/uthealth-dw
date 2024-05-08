/**************************************
 * This code strips Medicaid diagnosis codes of their punctuation (medicaid only)
 * 
 * Hot fix 4/11/24
 */

update data_warehouse.claim_diag_1_prt_mhtw
set diag_cd = regexp_replace(diag_cd, '[^a-zA-Z0-9]', '', 'g')
where diag_cd ~ '[^a-zA-Z0-9]';

update data_warehouse.claim_diag_1_prt_mcpp
set diag_cd = regexp_replace(diag_cd, '[^a-zA-Z0-9]', '', 'g')
where diag_cd ~ '[^a-zA-Z0-9]';

update data_warehouse.claim_diag_1_prt_mdcd
set diag_cd = regexp_replace(diag_cd, '[^a-zA-Z0-9]', '', 'g')
where diag_cd ~ '[^a-zA-Z0-9]';

update data_warehouse.claim_diag_1_prt_mcrt
set diag_cd = regexp_replace(diag_cd, '[^a-zA-Z0-9]', '', 'g')
where diag_cd ~ '[^a-zA-Z0-9]';

update data_warehouse.claim_diag_1_prt_mcrn
set diag_cd = regexp_replace(diag_cd, '[^a-zA-Z0-9]', '', 'g')
where diag_cd ~ '[^a-zA-Z0-9]';

vacuum analyze data_warehouse.claim_diag_1_prt_mhtw;
vacuum analyze data_warehouse.claim_diag_1_prt_mcpp;
vacuum analyze data_warehouse.claim_diag_1_prt_mdcd;
vacuum analyze data_warehouse.claim_diag_1_prt_mcrt;
vacuum analyze data_warehouse.claim_diag_1_prt_mcrn;


