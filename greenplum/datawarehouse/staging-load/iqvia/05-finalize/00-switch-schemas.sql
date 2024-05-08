/***********************************
 * This script does two things:
 * 	 1 - takes current DW Iqvia partitions and moves them into backup
 *   2 - swaps in updated Iqvia tables from dw_staging and moved them into DW proper
 * 
 * NOTE: sometimes the switch schema code has a lag of a few seconds.
 * DO NOT PANIC. Just check back in a minute or so. If it still didn't work, THEN panic.
 * 
 * Version control
 * 
 * Date		| Programmer 	| Notes
 * ********************************************
 * 04/16/24	| Isrrael		| Created using existing script for DW Truven
 * 
 * 
 ***********************************/

/********************
 * Drop old tables from backup, if they exist
 *******************/
drop table if exists backup.iqva_member_enrollment_monthly;
drop table if exists backup.iqva_member_enrollment_yearly;
drop table if exists backup.iqva_claim_detail;
drop table if exists backup.iqva_claim_diag;
drop table if exists backup.iqva_claim_header;
drop table if exists backup.iqva_claim_icd_proc;
drop table if exists backup.iqva_pharmacy_claims;


/********************
 * Create all the tables in backup
 *******************/
create table backup.iqva_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.iqva_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.iqva_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.iqva_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.iqva_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.iqva_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.iqva_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);


/*******************
 * Change owner of all tables to uthealth_dev
 * because otherwise the table exchange won't happen
 *******************/
alter table backup.iqva_member_enrollment_monthly owner to uthealth_dev;
alter table backup.iqva_member_enrollment_yearly owner to uthealth_dev;
alter table backup.iqva_claim_detail owner to uthealth_dev;
alter table backup.iqva_claim_diag owner to uthealth_dev;
alter table backup.iqva_claim_header owner to uthealth_dev;
alter table backup.iqva_claim_icd_proc owner to uthealth_dev;
alter table backup.iqva_pharmacy_claims owner to uthealth_dev;

alter table data_warehouse.member_enrollment_monthly owner to uthealth_dev;
alter table data_warehouse.member_enrollment_yearly owner to uthealth_dev;
alter table data_warehouse.claim_detail owner to uthealth_dev;
alter table data_warehouse.claim_diag owner to uthealth_dev;
alter table data_warehouse.claim_header owner to uthealth_dev;
alter table data_warehouse.claim_icd_proc owner to uthealth_dev;
alter table data_warehouse.pharmacy_claims owner to uthealth_dev;

alter table dw_staging.iqva_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.iqva_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.iqva_claim_detail owner to uthealth_dev;
alter table dw_staging.iqva_claim_diag owner to uthealth_dev;
alter table dw_staging.iqva_claim_header owner to uthealth_dev;
alter table dw_staging.iqva_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.iqva_pharmacy_claims owner to uthealth_dev;

/********************
 * Move current DW tables to backup
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition iqva
with table backup.iqva_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition iqva
with table backup.iqva_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition iqva
with table backup.iqva_claim_detail;

alter table data_warehouse.claim_diag
exchange partition iqva
with table backup.iqva_claim_diag;

alter table data_warehouse.claim_header
exchange partition iqva
with table backup.iqva_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition iqva
with table backup.iqva_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition iqva
with table backup.iqva_pharmacy_claims;

/********************
 * Meat and Potatoes: Move new tables into DW - iqva
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition iqva
with table dw_staging.iqva_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition iqva
with table dw_staging.iqva_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition iqva
with table dw_staging.iqva_claim_detail;

alter table data_warehouse.claim_diag
exchange partition iqva
with table dw_staging.iqva_claim_diag;

alter table data_warehouse.claim_header
exchange partition iqva
with table dw_staging.iqva_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition iqva
with table dw_staging.iqva_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition iqva
with table dw_staging.iqva_pharmacy_claims;


/********************************
 * update update_log
 *******************************/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--04/16/2024: Iqvia data first loaded to DW

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Iqvia data loaded up to 2023 Q2',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'member_enrollment%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%' or table_name like '%iqva')
	and table_name not like '%fiscal%'
	;

alter table data_warehouse.update_log owner to uthealth_analyst;
alter table backup.update_log owner to uthealth_analyst;

/*check
select *
from data_warehouse.update_log
where schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'member_enrollment%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%' or table_name like '%iqva')
	and table_name not like '%fiscal%'
order by table_name;
*/

--check 2
--select * from data_warehouse.update_log order by table_name;