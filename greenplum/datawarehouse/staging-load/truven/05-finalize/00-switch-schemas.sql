/***********************************
 * This script does two things:
 * 	 1 - takes current DW Truven partitions and moves them into backup
 *   2 - swaps in updated Truven tables from dw_staging and moved them into DW proper
 * 
 * NOTE: sometimes the switch schema code has a lag of a few seconds.
 * DO NOT PANIC. Just check back in a minute or so. If it still didn't work, THEN panic.
 * 
 * Version control
 * 
 * Date		| Programmer 	| Notes
 * ********************************************
 * 05/09/23	| Xiaorui		| Created
 * ********************************************
 * 08/03/23 | Xiaorui 		| Modified to accomodate truc and trum - note that on next data refresh,
 * 								we'll have to modify the backup portion of the scripts
 * ********************************************
 * 10/19/23 | Xiaorui 		| Modified the backup portion of the scripts... AS FORETOLD.
 * 
 * 
 ***********************************/

/********************
 * Drop old tables from backup, if they exist
 *******************/
drop table if exists backup.truc_member_enrollment_monthly;
drop table if exists backup.truc_member_enrollment_yearly;
drop table if exists backup.truc_claim_detail;
drop table if exists backup.truc_claim_diag;
drop table if exists backup.truc_claim_header;
drop table if exists backup.truc_claim_icd_proc;
drop table if exists backup.truc_pharmacy_claims;

drop table if exists backup.trum_member_enrollment_monthly;
drop table if exists backup.trum_member_enrollment_yearly;
drop table if exists backup.trum_claim_detail;
drop table if exists backup.trum_claim_diag;
drop table if exists backup.trum_claim_header;
drop table if exists backup.trum_claim_icd_proc;
drop table if exists backup.trum_pharmacy_claims;

/********************
 * Create all the tables in backup - commercial
 *******************/
create table backup.truc_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.truc_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.truc_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.truc_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.truc_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.truc_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.truc_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

/********************
 * Create all the tables in backup - Medicare advantage
 *******************/
create table backup.trum_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.trum_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.trum_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.trum_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.trum_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.trum_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.trum_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

/*******************
 * Change owner of all tables to uthealth_dev
 * because otherwise the table exchange won't happen
 *******************/
alter table backup.truc_member_enrollment_monthly owner to uthealth_dev;
alter table backup.truc_member_enrollment_yearly owner to uthealth_dev;
alter table backup.truc_claim_detail owner to uthealth_dev;
alter table backup.truc_claim_diag owner to uthealth_dev;
alter table backup.truc_claim_header owner to uthealth_dev;
alter table backup.truc_claim_icd_proc owner to uthealth_dev;
alter table backup.truc_pharmacy_claims owner to uthealth_dev;

alter table backup.trum_member_enrollment_monthly owner to uthealth_dev;
alter table backup.trum_member_enrollment_yearly owner to uthealth_dev;
alter table backup.trum_claim_detail owner to uthealth_dev;
alter table backup.trum_claim_diag owner to uthealth_dev;
alter table backup.trum_claim_header owner to uthealth_dev;
alter table backup.trum_claim_icd_proc owner to uthealth_dev;
alter table backup.trum_pharmacy_claims owner to uthealth_dev;

alter table data_warehouse.member_enrollment_monthly owner to uthealth_dev;
alter table data_warehouse.member_enrollment_yearly owner to uthealth_dev;
alter table data_warehouse.claim_detail owner to uthealth_dev;
alter table data_warehouse.claim_diag owner to uthealth_dev;
alter table data_warehouse.claim_header owner to uthealth_dev;
alter table data_warehouse.claim_icd_proc owner to uthealth_dev;
alter table data_warehouse.pharmacy_claims owner to uthealth_dev;

alter table dw_staging.truc_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.truc_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.truc_claim_detail owner to uthealth_dev;
alter table dw_staging.truc_claim_diag owner to uthealth_dev;
alter table dw_staging.truc_claim_header owner to uthealth_dev;
alter table dw_staging.truc_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.truc_pharmacy_claims owner to uthealth_dev;

alter table dw_staging.trum_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.trum_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.trum_claim_detail owner to uthealth_dev;
alter table dw_staging.trum_claim_diag owner to uthealth_dev;
alter table dw_staging.trum_claim_header owner to uthealth_dev;
alter table dw_staging.trum_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.trum_pharmacy_claims owner to uthealth_dev;

/********************
 * Move current DW tables to backup - commercial
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition truc
with table backup.truc_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition truc
with table backup.truc_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition truc
with table backup.truc_claim_detail;

alter table data_warehouse.claim_diag
exchange partition truc
with table backup.truc_claim_diag;

alter table data_warehouse.claim_header
exchange partition truc
with table backup.truc_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition truc
with table backup.truc_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition truc
with table backup.truc_pharmacy_claims;

/********************
 * Move current DW tables to backup - medicare advantage
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition trum
with table backup.trum_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition trum
with table backup.trum_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition trum
with table backup.trum_claim_detail;

alter table data_warehouse.claim_diag
exchange partition trum
with table backup.trum_claim_diag;

alter table data_warehouse.claim_header
exchange partition trum
with table backup.trum_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition trum
with table backup.trum_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition trum
with table backup.trum_pharmacy_claims;

/********************
 * Meat and Potatoes: Move new tables into DW - trum
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition trum
with table dw_staging.trum_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition trum
with table dw_staging.trum_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition trum
with table dw_staging.trum_claim_detail;

alter table data_warehouse.claim_diag
exchange partition trum
with table dw_staging.trum_claim_diag;

alter table data_warehouse.claim_header
exchange partition trum
with table dw_staging.trum_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition trum
with table dw_staging.trum_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition trum
with table dw_staging.trum_pharmacy_claims;

/********************
 * Meat and Potatoes: Move new tables into DW - truc
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition truc
with table dw_staging.truc_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition truc
with table dw_staging.truc_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition truc
with table dw_staging.truc_claim_detail;

alter table data_warehouse.claim_diag
exchange partition truc
with table dw_staging.truc_claim_diag;

alter table data_warehouse.claim_header
exchange partition truc
with table dw_staging.truc_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition truc
with table dw_staging.truc_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition truc
with table dw_staging.truc_pharmacy_claims;

/*******
 * QA: Did things swap over properly?

select month_year_id, count(*) from data_warehouse.member_enrollment_monthly_1_prt_truc
group by month_year_id
having count(*) > 1000
order by month_year_id desc limit 5;

--goes up to Dec 2022 so good, it worked

select month_year_id, count(*) from data_warehouse.member_enrollment_monthly_1_prt_trum
group by month_year_id
having count(*) > 100
order by month_year_id desc limit 5;

 */

/********************************
 * update update_log
 *******************************/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--08/03/2023: Truven data updated for Q3 2022

--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Truven data updated for 2023',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'member_enrollment%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%' or table_name like '%truc' or table_name like '%trum')
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
	and (table_name not like '%_1_%' or table_name like '%truc' or table_name like '%trum')
	and table_name not like '%fiscal%'
order by table_name;
*/

--check 2
--select * from data_warehouse.update_log order by table_name;


