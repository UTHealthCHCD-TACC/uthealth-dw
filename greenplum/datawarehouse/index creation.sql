



create index claim_diag_diag_cd_idx on data_warehouse.claim_diag using bitmap (diag_cd);

analyze data_warehouse.claim_diag;


create index claim_icd_proc_proc_cd_idx on data_warehouse.claim_icd_proc using bitmap (proc_cd);

analyze data_warehouse.claim_icd_proc;

