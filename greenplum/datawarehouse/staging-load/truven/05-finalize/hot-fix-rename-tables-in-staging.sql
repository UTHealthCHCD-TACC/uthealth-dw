/************************************
 * Hotfix:
 * 
 * Issuse is that the dw_staging tables were made as partitions, but they don't need to be
 * 
 * (in theory)
 * 
 * So I'm making then non-partitioned tables with consistent name structure of
 * 
 * truv_[dw table name]
 */

select max(month_year_id) from dw_staging.truv_member_enrollment_monthly;

/********************
 * Drop tables from dw_staging, if they exist
 *******************/
drop table if exists dw_staging.truv_claim_detail;
drop table if exists dw_staging.truv_claim_diag;
drop table if exists dw_staging.truv_claim_header;
drop table if exists dw_staging.truv_claim_icd_proc;
drop table if exists dw_staging.truv_pharmacy_claims;

/********************
 * Create nonpartitioned tables in dw_staging
 *******************/
--enrollment tables are already in correct format

create table dw_staging.truv_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.truv_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.truv_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.truv_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table dw_staging.truv_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

/********************
 * Creation scripts made owner uthealth_dev so change it to me
 */

alter table dw_staging.claim_detail owner to xrzhang;
alter table dw_staging.claim_diag owner to xrzhang;
alter table dw_staging.claim_header owner to xrzhang;
alter table dw_staging.claim_icd_proc owner to xrzhang;
alter table dw_staging.pharmacy_claims owner to xrzhang;

/********************
 * Move truven partition of staging tables out to nonpartitioned tables
 *******************/
/*alter table dw_staging.claim_detail
exchange partition truv
with table dw_staging.truv_claim_detail; */

alter table dw_staging.claim_diag
exchange partition truv
with table dw_staging.truv_claim_diag;

alter table dw_staging.claim_header
exchange partition truv
with table dw_staging.truv_claim_header;

alter table dw_staging.claim_icd_proc
exchange partition truv
with table dw_staging.truv_claim_icd_proc;

/*alter table dw_staging.pharmacy_claims
exchange partition truv
with table backup.truv_pharmacy_claims;*/

/**********************
 * Delete tables without truv_
 *********************/
--drop table if exists dw_staging.claim_detail;
drop table if exists dw_staging.claim_diag;
drop table if exists dw_staging.claim_header;
drop table if exists dw_staging.claim_icd_proc;
--drop table if exists dw_staging.pharmacy_claims;




