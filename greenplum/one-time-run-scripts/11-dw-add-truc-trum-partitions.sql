/************************************************
 * Script purpose:
 *  Add new partitions for truven commercial (truc) and truven medicare advantage (trum)
 *  Remove old 'truv' partition
 *
************************************************/

--monthly enrollment table
alter table data_warehouse.member_enrollment_monthly
add partition truc values ('truc');

alter table data_warehouse.member_enrollment_monthly
add partition trum values ('trum');

--yearly enrollment table
alter table data_warehouse.member_enrollment_yearly
add partition truc values ('truc');

alter table data_warehouse.member_enrollment_yearly
add partition trum values ('trum');

--claim detail
alter table data_warehouse.claim_detail
add partition truc values ('truc');

alter table data_warehouse.claim_detail
add partition trum values ('trum');

--claim diag
alter table data_warehouse.claim_diag
add partition truc values ('truc');

alter table data_warehouse.claim_diag
add partition trum values ('trum');

--claim header
alter table data_warehouse.claim_header
add partition truc values ('truc');

alter table data_warehouse.claim_header
add partition trum values ('trum');

--claim icd proc
alter table data_warehouse.claim_icd_proc
add partition truc values ('truc');

alter table data_warehouse.claim_icd_proc
add partition trum values ('trum');

--pharmacy claims
alter table data_warehouse.pharmacy_claims
add partition truc values ('truc');

alter table data_warehouse.pharmacy_claims
add partition trum values ('trum');


/********************************
 * Remove old 'truv' partition
 ********************************/

--monthly enrollment table
alter table data_warehouse.member_enrollment_monthly
drop partition truv;

--yearly enrollment table
alter table data_warehouse.member_enrollment_yearly
drop partition truv;

--claim detail
alter table data_warehouse.claim_detail
drop partition truv;

--claim diag
alter table data_warehouse.claim_diag
drop partition truv;

--claim header
alter table data_warehouse.claim_header
drop partition truv;

--claim icd proc
alter table data_warehouse.claim_icd_proc
drop partition truv;

--pharmacy claims
alter table data_warehouse.pharmacy_claims
drop partition truv;








