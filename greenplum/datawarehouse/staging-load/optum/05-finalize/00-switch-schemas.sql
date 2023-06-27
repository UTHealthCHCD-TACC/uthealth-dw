/***********************************
 * This script does two things:
 * 	 1 - takes current DW Optum partitions and moves them into backup
 *   2 - swaps in updated Optum tables from dw_staging and moved them into DW proper
 * 
 * NOTE: sometimes the switch schema code has a lag of a few seconds.
 * DO NOT PANIC. Just check back in a minute or so. If it still didn't work, THEN panic.
 * 
 * Version control
 * 
 * Date		|| Programmer 	|| Notes
 * ********************************************
 * 05/09/23	|| Xiaorui		|| Created
 * ********************************************
 * 06/27/23 || Isrrael		|| Took Truven version of file and converted for use for Optum tables
 * 
 ***********************************/

/********************
 * Drop old tables from backup, if they exist
 *******************/
drop table if exists backup.optd_member_enrollment_monthly;
drop table if exists backup.optd_member_enrollment_yearly;
drop table if exists backup.optd_claim_detail;
drop table if exists backup.optd_claim_diag;
drop table if exists backup.optd_claim_header;
drop table if exists backup.optd_claim_icd_proc;
drop table if exists backup.optd_pharmacy_claims;

drop table if exists backup.optz_member_enrollment_monthly;
drop table if exists backup.optz_member_enrollment_yearly;
drop table if exists backup.optz_claim_detail;
drop table if exists backup.optz_claim_diag;
drop table if exists backup.optz_claim_header;
drop table if exists backup.optz_claim_icd_proc;
drop table if exists backup.optz_pharmacy_claims;

/********************
 * Create all the tables in backup
 *******************/
create table backup.optd_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optd_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optd_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optd_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optd_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optd_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optd_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

-- OPTUM ZIP

create table backup.optz_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optz_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optz_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optz_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optz_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optz_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.optz_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

/*******************
 * Change owner of all tables to uthealth_dev
 * because otherwise the table exchange won't happen
 *******************/
alter table backup.optd_member_enrollment_monthly owner to uthealth_dev;
alter table backup.optd_member_enrollment_yearly owner to uthealth_dev;
alter table backup.optd_claim_detail owner to uthealth_dev;
alter table backup.optd_claim_diag owner to uthealth_dev;
alter table backup.optd_claim_header owner to uthealth_dev;
alter table backup.optd_claim_icd_proc owner to uthealth_dev;
alter table backup.optd_pharmacy_claims owner to uthealth_dev;

alter table dw_staging.optd_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.optd_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.optd_claim_detail owner to uthealth_dev;
alter table dw_staging.optd_claim_diag owner to uthealth_dev;
alter table dw_staging.optd_claim_header owner to uthealth_dev;
alter table dw_staging.optd_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.optd_pharmacy_claims owner to uthealth_dev;

-- OPTUM ZIP

alter table backup.optz_member_enrollment_monthly owner to uthealth_dev;
alter table backup.optz_member_enrollment_yearly owner to uthealth_dev;
alter table backup.optz_claim_detail owner to uthealth_dev;
alter table backup.optz_claim_diag owner to uthealth_dev;
alter table backup.optz_claim_header owner to uthealth_dev;
alter table backup.optz_claim_icd_proc owner to uthealth_dev;
alter table backup.optz_pharmacy_claims owner to uthealth_dev;

alter table dw_staging.optz_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.optz_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.optz_claim_detail owner to uthealth_dev;
alter table dw_staging.optz_claim_diag owner to uthealth_dev;
alter table dw_staging.optz_claim_header owner to uthealth_dev;
alter table dw_staging.optz_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.optz_pharmacy_claims owner to uthealth_dev;


/********************
 * Move current DW tables to backup
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition optd
with table backup.optd_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition optd
with table backup.optd_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition optd
with table backup.optd_claim_detail;

alter table data_warehouse.claim_diag
exchange partition optd
with table backup.optd_claim_diag;

alter table data_warehouse.claim_header
exchange partition optd
with table backup.optd_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition optd
with table backup.optd_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition optd
with table backup.optd_pharmacy_claims;

-- OPTUM ZIP

alter table data_warehouse.member_enrollment_monthly
exchange partition optz
with table backup.optz_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition optz
with table backup.optz_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition optz
with table backup.optz_claim_detail;

alter table data_warehouse.claim_diag
exchange partition optz
with table backup.optz_claim_diag;

alter table data_warehouse.claim_header
exchange partition optz
with table backup.optz_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition optz
with table backup.optz_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition optz
with table backup.optz_pharmacy_claims;


/********************
 * Meat and Potatoes: Move new tables into DW
 *******************/

alter table data_warehouse.member_enrollment_monthly
exchange partition optd
with table dw_staging.optd_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition optd
with table dw_staging.optd_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition optd
with table dw_staging.optd_claim_detail;

alter table data_warehouse.claim_diag
exchange partition optd
with table dw_staging.optd_claim_diag;

alter table data_warehouse.claim_header
exchange partition optd
with table dw_staging.optd_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition optd
with table dw_staging.optd_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition optd
with table dw_staging.optd_pharmacy_claims;

-- OPTUM ZIP

alter table data_warehouse.member_enrollment_monthly
exchange partition optz
with table dw_staging.optz_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition optz
with table dw_staging.optz_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition optz
with table dw_staging.optz_claim_detail;

alter table data_warehouse.claim_diag
exchange partition optz
with table dw_staging.optz_claim_diag;

alter table data_warehouse.claim_header
exchange partition optz
with table dw_staging.optz_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition optz
with table dw_staging.optz_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition optz
with table dw_staging.optz_pharmacy_claims;

/*******
 * QA: Did things swap over properly?

select month_year_id, count(*) from data_warehouse.member_enrollment_monthly
group by month_year_id
having count(*) > 100
order by month_year_id desc limit 5;

--goes up to June 2022 so good, it worked

202206	19183907
202205	19212620
202204	19148240
202203	19148870
202202	19148477

 */

/********************************
 * update update_log
 *******************************/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;


--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Optum data updated for ',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'member_enrollment%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%' or table_name like '%optd')
	and table_name not like '%fiscal%'
	;

/*check
select *
from data_warehouse.update_log
where schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'member_enrollment%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%' or table_name like '%optd')
	and table_name not like '%fiscal%'
order by table_name;
*/

--check 2
--select * from data_warehouse.update_log order by table_name;


