/************************************************
 * Script purpose:
 *  1) Add new partitions for chip perinatal and htw to dw tables
 *  2) re-label the data sources for claims originating from those member-enrl_months
 * 		a. Identify member-enrlmonths
 * 		b. Identify claims whose from_dos originates in those months
 * 		c. Relabel claims
 *
************************************************/

--monthly enrollment table
alter table data_warehouse.member_enrollment_monthly
add partition mcpp values ('mcpp');

alter table data_warehouse.member_enrollment_monthly
add partition mhtw values ('mhtw');

--yearly enrollment table
alter table data_warehouse.member_enrollment_yearly
add partition mhtw values ('mhtw');

alter table data_warehouse.member_enrollment_yearly
add partition mcpp values ('mcpp');

--claim detail
alter table data_warehouse.claim_detail
add partition mhtw values ('mhtw');

alter table data_warehouse.claim_detail
add partition mcpp values ('mcpp');

--claim diag
alter table data_warehouse.claim_diag
add partition mhtw values ('mhtw');

alter table data_warehouse.claim_diag
add partition mcpp values ('mcpp');

--claim header
alter table data_warehouse.claim_header
add partition mhtw values ('mhtw');

alter table data_warehouse.claim_header
add partition mcpp values ('mcpp');

--claim icd proc
alter table data_warehouse.claim_icd_proc
add partition mhtw values ('mhtw');

alter table data_warehouse.claim_icd_proc
add partition mcpp values ('mcpp');

/*******************
 * YOU STOPPED HERE, NEED TO RELABEL CLAIMS
 */












