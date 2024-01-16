/***********************************
 * This script does two things:
 * 	 1 - takes current DW Medicare partitions and moves them into backup
 *   2 - swaps in updated Medicare tables from dw_staging and moved them into DW proper
 * 
 * NOTE: sometimes the switch schema code has a lag of a few seconds.
 * DO NOT PANIC. Just check back in a minute or so. If it still didn't work, THEN panic.
 * 
 * Version control
 * 
 * Date		|| Programmer 	|| Notes
 * ********************************************
 * 06/27/23	|| Xiaorui		|| Created
 * 10/09/23 || Xiaorui		|| Update after claims tables refresh
 * 12/19/23 || Xiaorui		|| Last update left out enrollment tables - modified script so enroll tables are back in
 * 							   If need to just update claims table again, consider splitting into 2 scripts
 * 
 ***********************************/

/********************
 * Drop old tables from backup, if they exist
 *******************/
drop table if exists backup.mcrn_member_enrollment_monthly;
drop table if exists backup.mcrn_member_enrollment_yearly;
drop table if exists backup.mcrn_claim_detail;
drop table if exists backup.mcrn_claim_diag;
drop table if exists backup.mcrn_claim_header;
drop table if exists backup.mcrn_claim_icd_proc;
drop table if exists backup.mcrn_pharmacy_claims;

/********************
 * Create all the tables in backup
 *******************/

create table backup.mcrn_member_enrollment_monthly
(like data_warehouse.member_enrollment_monthly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcrn_member_enrollment_yearly
(like data_warehouse.member_enrollment_yearly including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcrn_claim_detail
(like data_warehouse.claim_detail including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcrn_claim_diag
(like data_warehouse.claim_diag including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcrn_claim_header
(like data_warehouse.claim_header including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcrn_claim_icd_proc
(like data_warehouse.claim_icd_proc including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

create table backup.mcrn_pharmacy_claims
(like data_warehouse.pharmacy_claims including defaults) 
with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=5)
distributed by (uth_member_id);

/*******************
 * Change owner of all tables to uthealth_dev
 * because otherwise the table exchange won't happen
 *******************/
alter table backup.mcrn_member_enrollment_monthly owner to uthealth_dev;
alter table backup.mcrn_member_enrollment_yearly owner to uthealth_dev;
alter table backup.mcrn_claim_detail owner to uthealth_dev;
alter table backup.mcrn_claim_diag owner to uthealth_dev;
alter table backup.mcrn_claim_header owner to uthealth_dev;
alter table backup.mcrn_claim_icd_proc owner to uthealth_dev;
alter table backup.mcrn_pharmacy_claims owner to uthealth_dev;

alter table dw_staging.mcrn_member_enrollment_monthly owner to uthealth_dev;
alter table dw_staging.mcrn_member_enrollment_yearly owner to uthealth_dev;
alter table dw_staging.mcrn_claim_detail owner to uthealth_dev;
alter table dw_staging.mcrn_claim_diag owner to uthealth_dev;
alter table dw_staging.mcrn_claim_header owner to uthealth_dev;
alter table dw_staging.mcrn_claim_icd_proc owner to uthealth_dev;
alter table dw_staging.mcrn_pharmacy_claims owner to uthealth_dev;

alter table data_warehouse.member_enrollment_monthly owner to uthealth_dev;
alter table data_warehouse.member_enrollment_yearly owner to uthealth_dev;
alter table data_warehouse.claim_detail owner to uthealth_dev;
alter table data_warehouse.claim_diag owner to uthealth_dev;
alter table data_warehouse.claim_header owner to uthealth_dev;
alter table data_warehouse.claim_icd_proc owner to uthealth_dev;
alter table data_warehouse.pharmacy_claims owner to uthealth_dev;

/********************
 * Move current DW tables to backup
 *******************/
alter table data_warehouse.member_enrollment_monthly
exchange partition mcrn
with table backup.mcrn_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition mcrn
with table backup.mcrn_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition mcrn
with table backup.mcrn_claim_detail;

alter table data_warehouse.claim_diag
exchange partition mcrn
with table backup.mcrn_claim_diag;

alter table data_warehouse.claim_header
exchange partition mcrn
with table backup.mcrn_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition mcrn
with table backup.mcrn_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition mcrn
with table backup.mcrn_pharmacy_claims;


/********************
 * Meat and Potatoes: Move new tables into DW
 *******************/
alter table data_warehouse.member_enrollment_monthly
exchange partition mcrn
with table dw_staging.mcrn_member_enrollment_monthly;

alter table data_warehouse.member_enrollment_yearly
exchange partition mcrn
with table dw_staging.mcrn_member_enrollment_yearly;

alter table data_warehouse.claim_detail
exchange partition mcrn
with table dw_staging.mcrn_claim_detail;

alter table data_warehouse.claim_diag
exchange partition mcrn
with table dw_staging.mcrn_claim_diag;

alter table data_warehouse.claim_header
exchange partition mcrn
with table dw_staging.mcrn_claim_header;

alter table data_warehouse.claim_icd_proc
exchange partition mcrn
with table dw_staging.mcrn_claim_icd_proc;

alter table data_warehouse.pharmacy_claims
exchange partition mcrn
with table dw_staging.mcrn_pharmacy_claims;


/*******
 * QA: Did things swap over properly?
 * 
select year, count(*) from medicare_national.mbsf_abcd_summary
group by 1 order by 1;

2014	2877408
2015	2963123
2016	3046466
2017	3133013
2018	3229338
2019	3306928
2020	3390959
2021	3248859

select month_year_id, count(*) from data_warehouse.member_enrollment_monthly
where data_source = 'mcrn'
group by month_year_id
having count(*) > 100
order by month_year_id desc limit 5;

--goes up to Dec 2020

202008	3238233
202009	3243906
202010	3249748
202011	3252950
202012	3255361

 */

/********************************
 * update update_log
 *******************************/

--backup update_log
drop table if exists backup.update_log;

create table backup.update_log as
select * from data_warehouse.update_log;

--06/27/2023: Medicare enrollment tables refreshed
--10/09/2023: Medicare claims tables refreshed

/*
--update update_log for enrollment tables only
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Medicare enrollment tables refreshed',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse'
	and table_name like 'member_enrollment%'
	and (table_name not like '%_1_%' or table_name like '%mcrt')
	and table_name not like '%fiscal%'
	;
*/

/*
--update update log for claims tables only
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Medicare claims tables refreshed',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%' or table_name like '%mcrt')
	and table_name not like '%fiscal%'
	;
*/


--update update_log
update data_warehouse.update_log a
set data_last_updated = current_date,
	details = 'Medicare data updated for 2020/2021',
	last_vacuum_analyze = case when b.last_vacuum is not null then b.last_vacuum else b.last_analyze end
from pg_catalog.pg_stat_all_tables b
where a.schema_name = b.schemaname and a.table_name = b.relname
and schema_name = 'data_warehouse'
	and (table_name like 'claim%' or 
		table_name like 'member_enrollment%' or 
		table_name like 'pharmacy_claims%')
	and (table_name not like '%_1_%' or table_name like '%mcrn')
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
	and (table_name not like '%_1_%' or table_name like '%mcrn')
	and table_name not like '%fiscal%'
order by table_name;
*/

--check 2
--select * from data_warehouse.update_log order by table_name;


