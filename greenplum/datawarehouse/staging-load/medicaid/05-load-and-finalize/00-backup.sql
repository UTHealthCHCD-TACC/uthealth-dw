
/* ******************************************************************************************************
 *  load claim detail for Medicaid
 * ******************************************************************************************************
 *  Author  || Date       || Notes
 * ******************************************************************************************************
 * ******************************************************************************************************
 *  wc001   || 9/29/2021  || script creation - pointed at dev schema for testing
 * ****************************************************************************************************** 
 *  wc002   || 12/9/2021  || modify to do end script
 * ******************************************************************************************************
 *  xiaorui || 09/12/2023 || added claim_status
 * ****************************************************************************************************** 
 * */


---Create backup schema

drop schema if exists backup_stage cascade;
create schema backup_stage;


--- Backup claim header partition
drop table if exists backup_stage.claim_header_1_prt_mdcd;

create table backup_stage.claim_header_1_prt_mdcd
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
    as table data_warehouse.claim_header_1_prt_mdcd
   distributed by (uth_member_id);
  
analyze backup_stage.claim_header_1_prt_mdcd;
  
--- Backup claim detail partition
drop table if exists backup_stage.claim_detail_1_prt_mdcd;

create table backup_stage.claim_detail_1_prt_mdcd
with (
		appendonly=true,
		orientation=column,
		compresstype=zlib,
		compresslevel=5
	 )
    as table data_warehouse.claim_detail_1_prt_mdcd
   distributed by (uth_member_id);
  
analyze backup_stage.claim_detail_1_prt_mdcd;


--- Backup claim diag partition
drop table if exists backup_stage.claim_diag_1_prt_mdcd;

create table backup_stage.claim_diag_1_prt_mdcd
with (
		appendonly=true,
		orientation=column,
		compresstype=zlib,
		compresslevel=5
	 )
    as table data_warehouse.claim_diag_1_prt_mdcd
   distributed by (uth_member_id);
  
analyze backup_stage.claim_diag_1_prt_mdcd;


--- Backup claim icd partition
drop table if exists backup_stage.claim_icd_proc_1_prt_mdcd;

create table backup_stage.claim_icd_proc_1_prt_mdcd
with (
		appendonly=true,
		orientation=column,
		compresstype=zlib,
		compresslevel=5
	 )
    as table data_warehouse.claim_icd_proc_1_prt_mdcd
   distributed by (uth_member_id);
  
analyze backup_stage.claim_icd_proc_1_prt_mdcd;


--- Backup enrollment monthly
drop table if exists backup_stage.member_enrollment_monthly_1_prt_mdcd;

create table backup_stage.member_enrollment_monthly_1_prt_mdcd
with (
		appendonly=true,
		orientation=column,
		compresstype=zlib,
		compresslevel=5
	 )
    as table data_warehouse.member_enrollment_monthly_1_prt_mdcd
   distributed by (uth_member_id);
  
analyze backup_stage.member_enrollment_monthly_1_prt_mdcd;
  
--- Backup yearly monthly
drop table if exists backup_stage.member_enrollment_yearly_1_prt_mdcd;

create table backup_stage.member_enrollment_yearly_1_prt_mdcd
with (
		appendonly=true,
		orientation=column,
		compresstype=zlib,
		compresslevel=5
	 )
    as table data_warehouse.member_enrollment_yearly_1_prt_mdcd
   distributed by (uth_member_id);
  
analyze backup_stage.member_enrollment_yearly_1_prt_mdcd;


--- Backup yearly monthly
drop table if exists backup_stage.member_enrollment_yearly_1_prt_mdcd;

create table backup_stage.member_enrollment_yearly_1_prt_mdcd
with (
		appendonly=true,
		orientation=column,
		compresstype=zlib,
		compresslevel=5
	 )
    as table data_warehouse.member_enrollment_yearly_1_prt_mdcd
   distributed by (uth_member_id);
  
analyze backup_stage.member_enrollment_yearly_1_prt_mdcd;


--- Backup pharmacy
drop table if exists backup_stage.pharmacy_claims_1_prt_mdcd;

create table backup_stage.pharmacy_claims_1_prt_mdcd
with (
		appendonly=true,
		orientation=column,
		compresstype=zlib,
		compresslevel=5
	 )
    as table data_warehouse.pharmacy_claims_1_prt_mdcd
   distributed by (uth_member_id);
  
analyze backup_stage.pharmacy_claims_1_prt_mdcd;